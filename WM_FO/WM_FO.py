"""
WM_FO - Aplicación del operario de campo.

Uso:  python WM_FO.py <ip_broker> <puerto_broker> <id_operador> [<fichero_servicios>]

  - Solicita riegos a CENTRAL por Kafka y muestra cada paso (recibida, autorizada,
    denegada), la telemetría de la estación mientras riega y el resumen final.
  - Muestra el estado de todas las estaciones (CENTRAL lo publica cada 2 s).
  - Con un fichero de servicios los solicita en orden, esperando 4 s entre uno y otro.
    Formato de cada línea: <WS_ID> <duración_en_segundos>   (# para comentarios)
"""

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import kafka_utils as K   # noqa: E402

TIMEOUT_AUTORIZACION = 15   # s esperando a que CENTRAL autorice o deniegue
MARGEN_FIN = 15             # s extra sobre la duración esperando el resumen final
PAUSA_ENTRE_SERVICIOS = 4

MENU = """
=== OPERARIO {id} ===
  1  Solicitar riego
  2  Ver estaciones
  3  Ejecutar fichero de servicios
  0  Salir"""


def log(texto):
    print(f"{time.strftime('%H:%M:%S')} {texto}", flush=True)


class Operario:
    def __init__(self, servidor_kafka, operator_id):
        self.servidor_kafka = servidor_kafka
        self.id = operator_id
        self.estaciones = []          # última foto publicada por CENTRAL
        self.orden = None             # solicitud en curso: {'order_id', 'ws_id'}
        self.decidida = threading.Event()    # CENTRAL ha autorizado o denegado
        self.terminada = threading.Event()

        self.productor = K.crear_productor(servidor_kafka, log)
        listo = threading.Event()
        threading.Thread(target=self.bucle_kafka, args=(listo,), daemon=True).start()
        listo.wait()

    def bucle_kafka(self, listo):
        topics = [K.TOPIC_CENTRAL, K.TOPIC_TELEMETRIA]
        for topic, msg in K.escuchar(self.servidor_kafka, topics, K.nuevo_id(f'fo-{self.id}'), log, listo):
            orden = self.orden
            de_mi_orden = orden is not None and msg.get('order_id') == orden['order_id']
            if topic == K.TOPIC_CENTRAL:
                # Las ORDEN van dirigidas a los Engines: el operario las ignora
                if msg.get('tipo') == K.TIPO_ESTADO_RED:
                    self.estaciones = msg['estaciones']
                elif msg.get('tipo') == K.TIPO_NOTIFICACION and de_mi_orden:
                    self._notificacion(msg)
            elif topic == K.TOPIC_TELEMETRIA and de_mi_orden:
                log(f"  [{msg['ws_id']}] {msg['segundos']} s | caudal {msg['caudal']} L/min | "
                    f"acumulado {msg['volumen']} L")

    def _notificacion(self, msg):
        tipo = msg['subtipo']
        if tipo == 'RESUMEN':
            print(f"\n  ======== RESUMEN DE RIEGO {msg['ws_id']} ========\n"
                  f"  Motivo de fin : {msg['motivo']}\n"
                  f"  Duración      : {msg['segundos']} s\n"
                  f"  Volumen total : {msg['volumen']} L\n"
                  f"  ============================================\n", flush=True)
            self.decidida.set()
            self.terminada.set()
        else:
            log(f"[CENTRAL] {tipo}: {msg['texto']}")
            if tipo in ('AUTORIZADA', 'DENEGADA'):
                self.decidida.set()
            if tipo == 'DENEGADA':
                self.terminada.set()

    def solicitar(self, ws_id, duracion):
        """Envía una solicitud y espera a que termine (denegada o resumen final)."""
        self.orden = {'order_id': K.nuevo_id('ORD'), 'ws_id': ws_id}
        self.decidida.clear()
        self.terminada.clear()
        log(f"[FO] Solicitando riego en {ws_id} durante {duracion} s...")
        self.productor.send(K.TOPIC_SOLICITUDES, key=ws_id, value={
            'order_id': self.orden['order_id'], 'operator_id': self.id,
            'ws_id': ws_id, 'duracion': duracion, 'origen': 'FO'})
        self.productor.flush()

        # Primero esperamos a que CENTRAL decida y luego a que termine el riego
        if not self.decidida.wait(TIMEOUT_AUTORIZACION):
            log("[FO] CENTRAL no ha respondido a la solicitud")
        elif not self.terminada.wait(duracion + MARGEN_FIN):
            log("[FO] No se ha recibido el resumen final del riego")
        self.orden = None

    def ver_estaciones(self):
        if not self.estaciones:
            print("  (aún no se ha recibido el estado de la red desde CENTRAL)")
        for ws in self.estaciones:
            extra = f" | {ws['caudal']} L/min, {ws['volumen']} L, {ws['operario']}" if ws['estado'] == 'REGANDO' else ''
            print(f"  {ws['id']:<8} {ws['estado']:<15} {ws['ubicacion']}{extra}")

    def ejecutar_fichero(self, ruta):
        try:
            with open(ruta, encoding='utf-8') as f:
                servicios = [l.split() for l in f if l.strip() and not l.lstrip().startswith('#')]
        except OSError as e:
            log(f"[FO] No se puede leer {ruta}: {e}")
            return
        for i, servicio in enumerate(servicios, 1):
            if len(servicio) != 2 or not servicio[1].isdigit():
                log(f"[FO] Línea ignorada (formato <WS_ID> <segundos>): {' '.join(servicio)}")
                continue
            print(f"\n--- Servicio {i}/{len(servicios)} ---", flush=True)
            self.solicitar(servicio[0], int(servicio[1]))
            if i < len(servicios):
                log(f"[FO] Esperando {PAUSA_ENTRE_SERVICIOS} s antes del siguiente servicio...")
                time.sleep(PAUSA_ENTRE_SERVICIOS)
        log("[FO] Fichero de servicios completado")

    def menu(self):
        while True:
            print(MENU.format(id=self.id), flush=True)
            try:
                opcion = input('> ').strip()
                if opcion == '1':
                    ws_id = input('  ID de la estación: ').strip()
                    duracion = input('  Duración (s): ').strip()
                    if ws_id and duracion.isdigit():
                        self.solicitar(ws_id, int(duracion))
                    else:
                        print('  Datos no válidos')
                elif opcion == '2':
                    self.ver_estaciones()
                elif opcion == '3':
                    self.ejecutar_fichero(input('  Ruta del fichero: ').strip())
                elif opcion == '0':
                    return
            except EOFError:
                return


if __name__ == '__main__':
    if len(sys.argv) not in (4, 5):
        print('Uso: python WM_FO.py <ip_broker> <puerto_broker> <id_operador> [<fichero_servicios>]')
        sys.exit(1)
    try:
        operario = Operario(K.bootstrap(sys.argv[1], sys.argv[2]), sys.argv[3])
        if len(sys.argv) == 5:
            operario.ejecutar_fichero(sys.argv[4])
        operario.menu()
    except KeyboardInterrupt:
        print('\n[FO] Apagado.')
