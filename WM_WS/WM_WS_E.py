"""
WM_WS_E - Engine de la estación de riego (caudalímetro + electroválvula).

Uso:  python WM_WS_E.py <ip_broker> <puerto_broker> <ip_monitor> <puerto_monitor>

  - Se conecta a su Monitor por sockets. El Monitor le asigna el ID de la estación
    y le pregunta su salud cada segundo (responde OK o KO).
  - Por Kafka recibe las órdenes de CENTRAL (REGAR, PARAR, BLOQUEAR, ACTIVAR) y
    publica eventos de riego y la telemetría (caudal y volumen) cada segundo.
  - Menú en consola para regar manualmente, parar el riego y simular una fuga (KO).
"""

import os
import random
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import estados as E                                         # noqa: E402
from common import kafka_utils as K                                     # noqa: E402
from common.protocolo import CanalTramas, ErrorProtocolo, STX, construir, separar  # noqa: E402

CAUDAL_NOMINAL = 15.0    # L/min

MENU = """
  r <seg>  Solicitar riego manual en esta estación (p.ej. 'r 10')
  p        Parar el riego en curso
  k        Simular avería / fuga (el Engine responde KO al Monitor)
  o        Avería resuelta (vuelve a responder OK)
  e        Ver estado de la estación
  (Enter)  Mostrar este menú
"""


def log(texto):
    print(f"{time.strftime('%H:%M:%S')} {texto}", flush=True)


class Engine:
    def __init__(self, servidor_kafka, ip_monitor, puerto_monitor):
        self.servidor_kafka = servidor_kafka
        self.monitor = (ip_monitor, puerto_monitor)
        self.ws_id = None
        self.lock = threading.Lock()
        self.salud = E.SALUD_OK
        self.bloqueada = False
        self.riego = None            # dict del riego en curso
        self.motivo_parada = None    # si no es None, el bucle de riego debe parar
        self.productor = None
        self.id_asignado = threading.Event()

    def estado(self):
        if self.salud == E.SALUD_KO:
            return E.FUGA
        if self.bloqueada:
            return E.FUERA_SERVICIO
        return E.REGANDO if self.riego else E.DISPONIBLE

    def publicar(self, topic, mensaje):
        self.productor.send(topic, key=self.ws_id, value={'ws_id': self.ws_id, **mensaje})

    # -- Conexión con el Monitor (sockets) --------------------------------------
    def bucle_monitor(self):
        while True:
            canal = None
            try:
                canal = CanalTramas(socket.create_connection(self.monitor, timeout=5), timeout=None)
                canal.saludar()
                campos = separar(canal.peticion('HOLA'))
                if campos[0] != 'ID':
                    raise ErrorProtocolo(f"Respuesta inesperada del Monitor: {campos}")
                if self.ws_id not in (None, campos[1]):
                    log(f"[ENGINE] Aviso: el Monitor cambia el ID {self.ws_id} -> {campos[1]}")
                self.ws_id = campos[1]
                log(f"[ENGINE] Conectado al Monitor. ID asignado: {self.ws_id}")
                self.id_asignado.set()

                # A partir de aquí el Monitor pregunta y nosotros respondemos
                while True:
                    byte = canal.leer_control()
                    if byte != STX:
                        continue
                    if canal.recibir(byte) == 'SALUD':
                        canal.enviar(self.salud)
            except (ErrorProtocolo, OSError) as e:
                log(f"[ENGINE] Sin conexión con el Monitor ({e}). Reintento en 3s...")
                self.parar_riego(E.FIN_DESCONEXION)
                if canal:
                    canal.cerrar()
                time.sleep(3)

    # -- Órdenes de CENTRAL (Kafka) ---------------------------------------------
    def bucle_kafka(self):
        self.id_asignado.wait()
        self.productor = K.crear_productor(self.servidor_kafka, log)
        listo = threading.Event()
        mensajes = K.escuchar(self.servidor_kafka, [K.TOPIC_CENTRAL],
                              K.nuevo_id(f'engine-{self.ws_id}'), log, listo)
        threading.Thread(target=self._anunciar, args=(listo,), daemon=True).start()
        for _, msg in mensajes:
            # Solo nos interesan las órdenes y notificaciones de esta estación (no ESTADO_RED)
            if msg.get('ws_id') != self.ws_id:
                continue
            if msg.get('tipo') == K.TIPO_NOTIFICACION:
                log(f"[CENTRAL] {msg['subtipo']}: {msg['texto']}")
            elif msg.get('tipo') == K.TIPO_ORDEN:
                self.atender_orden(msg)

    def _anunciar(self, listo):
        listo.wait()
        self.publicar(K.TOPIC_EVENTOS_WS, {'evento': 'ENGINE_LISTO'})

    def atender_orden(self, orden):
        tipo = orden.get('orden')
        if tipo == 'REGAR':
            self.iniciar_riego(orden)
        elif tipo == 'PARAR':
            self.parar_riego(orden.get('motivo', E.FIN_PARADA_CENTRAL))
        elif tipo == 'BLOQUEAR':
            if not self.bloqueada:
                log("[ENGINE] *** FUERA DE SERVICIO por orden de CENTRAL ***")
            self.bloqueada = True
            self.parar_riego(E.FIN_BLOQUEO)
        elif tipo == 'ACTIVAR':
            if self.bloqueada:
                log("[ENGINE] Estación ACTIVADA por CENTRAL")
            self.bloqueada = False

    # -- Riego ------------------------------------------------------------------
    def iniciar_riego(self, orden):
        with self.lock:
            if self.riego:
                motivo = 'ya hay un riego en curso'
            elif self.salud == E.SALUD_KO:
                motivo = 'fuga detectada'
            elif self.bloqueada:
                motivo = 'estación fuera de servicio'
            else:
                motivo = None
                riego = {'order_id': orden['order_id'], 'operator_id': orden['operator_id'],
                         'duracion': int(orden['duracion'])}
                self.riego, self.motivo_parada = riego, None
        if motivo:
            log(f"[ENGINE] Riego rechazado: {motivo}")
            self.publicar(K.TOPIC_EVENTOS_WS, {'evento': 'RIEGO_RECHAZADO',
                                               'order_id': orden['order_id'], 'motivo': motivo})
            return
        self.publicar(K.TOPIC_EVENTOS_WS, {'evento': 'RIEGO_INICIADO', 'order_id': orden['order_id']})
        threading.Thread(target=self._regar, args=(riego,), daemon=True).start()

    def parar_riego(self, motivo):
        with self.lock:
            if self.riego and not self.motivo_parada:
                self.motivo_parada = motivo

    def _regar(self, riego):
        log(f"[ENGINE] >>> ELECTROVÁLVULA ABIERTA ({riego['duracion']} s, operario {riego['operator_id']})")
        volumen, segundos = 0.0, 0
        while segundos < riego['duracion'] and not self.motivo_parada:
            time.sleep(1)
            if self.motivo_parada:
                break
            segundos += 1
            caudal = round(CAUDAL_NOMINAL + random.uniform(-0.5, 0.5), 2)   # lectura del caudalímetro
            volumen = round(volumen + caudal / 60, 2)
            self.publicar(K.TOPIC_TELEMETRIA, {'order_id': riego['order_id'], 'caudal': caudal,
                                               'volumen': volumen, 'segundos': segundos})
            log(f"[ENGINE] Regando {segundos}/{riego['duracion']} s | {caudal} L/min | {volumen} L")

        with self.lock:
            motivo = self.motivo_parada or E.FIN_COMPLETADO
            self.riego, self.motivo_parada = None, None
        log(f"[ENGINE] <<< ELECTROVÁLVULA CERRADA ({motivo}): {volumen} L en {segundos} s")
        self.publicar(K.TOPIC_EVENTOS_WS, {'evento': 'RIEGO_FINALIZADO', 'order_id': riego['order_id'],
                                           'motivo': motivo, 'segundos': segundos, 'volumen': volumen})

    # -- Menú de la estación ----------------------------------------------------
    def bucle_menu(self):
        print(MENU, flush=True)
        while True:
            try:
                partes = input().strip().lower().split()
            except EOFError:     # sin consola (p.ej. docker sin -it): sin menú
                return
            cmd = partes[0] if partes else ''
            if cmd == 'k':
                self.salud = E.SALUD_KO
                log("[ENGINE] !!! FUGA simulada: se responde KO al Monitor")
                self.parar_riego(E.FIN_FUGA)
            elif cmd == 'o':
                self.salud = E.SALUD_OK
                log("[ENGINE] Avería resuelta: se responde OK al Monitor")
            elif cmd == 'p':
                if self.riego:
                    self.parar_riego(E.FIN_PARADA_MANUAL)
                else:
                    log("[ENGINE] No hay ningún riego en curso")
            elif cmd == 'r' and len(partes) == 2 and partes[1].isdigit() and self.productor:
                self.publicar(K.TOPIC_SOLICITUDES, {'order_id': K.nuevo_id('ORD'), 'origen': 'ESTACION',
                                                    'operator_id': f'{self.ws_id}-LOCAL',
                                                    'duracion': int(partes[1])})
                log(f"[ENGINE] Solicitud de riego manual ({partes[1]} s) enviada a CENTRAL")
            elif cmd == 'e':
                log(f"[ENGINE] {self.ws_id}: {self.estado()} | salud {self.salud}")
            else:   # Enter vacío o comando desconocido
                print(MENU, flush=True)
                log(f"[ENGINE] {self.ws_id}: {self.estado()} | salud {self.salud}")

    def iniciar(self):
        threading.Thread(target=self.bucle_monitor, daemon=True).start()
        threading.Thread(target=self.bucle_kafka, daemon=True).start()
        self.bucle_menu()
        threading.Event().wait()   # sin menú, seguimos funcionando


if __name__ == '__main__':
    if len(sys.argv) != 5:
        print('Uso: python WM_WS_E.py <ip_broker> <puerto_broker> <ip_monitor> <puerto_monitor>')
        sys.exit(1)
    try:
        Engine(K.bootstrap(sys.argv[1], sys.argv[2]), sys.argv[3], int(sys.argv[4])).iniciar()
    except KeyboardInterrupt:
        print('\n[ENGINE] Apagado.')
