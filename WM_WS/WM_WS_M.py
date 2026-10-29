"""
WM_WS_M - Monitor de la estación de riego.

Uso:  python WM_WS_M.py <puerto_engine> <ip_central> <puerto_central> <ws_id> [<ubicación>] [--registrar]

  - Se conecta a CENTRAL por sockets y se autentica con AUTH#<ws_id>#<ubicación>#<salud>.
  - Si no está registrada previamente en CENTRAL, CENTRAL denegará la autenticación salvo
    que se haya dado de alta en su panel web o se pase el argumento opcional --registrar.
  - Ofrece un servidor de sockets al Engine; al conectarse le asigna el ID con ID#<ws_id>
    y después le envía SALUD cada segundo. Si el Engine responde KO o no responde,
    la estación tiene una avería (fuga) y se notifica a CENTRAL con ESTADO#<ws_id>#KO.
  - A CENTRAL le envía ESTADO#<ws_id>#<OK|KO> en cuanto cambia la salud y, además,
    cada 2 segundos como latido para que CENTRAL sepa que la estación sigue viva.
"""

import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import estados as E                                         # noqa: E402
from common.protocolo import CanalTramas, ErrorProtocolo, construir, separar  # noqa: E402

INTERVALO_SALUD = 1      # s entre comprobaciones al Engine
TIMEOUT_ENGINE = 3       # s sin respuesta del Engine -> avería
INTERVALO_LATIDO = 2     # s entre latidos a CENTRAL


def log(texto):
    print(f"{time.strftime('%H:%M:%S')} {texto}", flush=True)


class Monitor:
    def __init__(self, puerto_engine, ip_central, puerto_central, ws_id, ubicacion, auto_registrar=False):
        self.puerto_engine = puerto_engine
        self.central = (ip_central, puerto_central)
        self.ws_id = ws_id
        self.ubicacion = ubicacion
        self.auto_registrar = auto_registrar
        # Mientras no haya un Engine conectado y respondiendo OK la estación no es segura
        self.salud = E.SALUD_KO
        self.cambio_salud = threading.Event()

    def _set_salud(self, salud, motivo):
        if salud != self.salud:
            self.salud = salud
            log(f"[MONITOR {self.ws_id}] Salud -> {salud} ({motivo})")
            self.cambio_salud.set()

    # -- Conexión con CENTRAL ---------------------------------------------------
    def bucle_central(self):
        while True:
            canal = None
            try:
                log(f"[MONITOR {self.ws_id}] Conectando a CENTRAL {self.central[0]}:{self.central[1]}...")
                canal = CanalTramas(socket.create_connection(self.central, timeout=5), timeout=5)
                canal.saludar()

                if self.auto_registrar:
                    reg_resp = separar(canal.peticion(construir('REGISTRO', self.ws_id, self.ubicacion)))
                    log(f"[MONITOR {self.ws_id}] Petición de alta enviada a CENTRAL: {reg_resp}")

                respuesta = separar(canal.peticion(construir('AUTH', self.ws_id, self.ubicacion, self.salud)))
                if respuesta[0] != 'OK':
                    log(f"[MONITOR {self.ws_id}] CENTRAL rechazó la autenticación: {respuesta[1:]}. "
                        f"(Si es WS_NO_REGISTRADA, déla de alta en el panel de CENTRAL o use --registrar)")
                    canal.despedir()
                    time.sleep(5)
                    continue
                log(f"[MONITOR {self.ws_id}] Autenticado en CENTRAL")

                while True:
                    self.cambio_salud.wait(INTERVALO_LATIDO)
                    self.cambio_salud.clear()
                    canal.peticion(construir('ESTADO', self.ws_id, self.salud))
            except (ErrorProtocolo, OSError) as e:
                log(f"[MONITOR {self.ws_id}] Imposible conectar con WM_Central ({e}). Reintento en 3s...")
                if canal:
                    canal.cerrar()
                time.sleep(3)

    # -- Servidor para el Engine ------------------------------------------------
    def bucle_engine(self):
        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind(('0.0.0.0', self.puerto_engine))
        servidor.listen(1)
        log(f"[MONITOR {self.ws_id}] Esperando al Engine en el puerto {self.puerto_engine}...")
        while True:
            conn, addr = servidor.accept()
            log(f"[MONITOR {self.ws_id}] Engine conectado desde {addr[0]}")
            canal = CanalTramas(conn, timeout=TIMEOUT_ENGINE)
            try:
                canal.esperar_saludo()
                # El Engine no conoce su ID: se lo asignamos nosotros
                canal.recibir()                       # HOLA del Engine
                canal.enviar(construir('ID', self.ws_id))
                while True:
                    self._set_salud(self._comprobar(canal), 'respuesta del Engine')
                    time.sleep(INTERVALO_SALUD)
            except socket.timeout:
                log(f"[MONITOR {self.ws_id}] El Engine no responde")
            except (ErrorProtocolo, OSError) as e:
                log(f"[MONITOR {self.ws_id}] Conexión con el Engine perdida ({e})")
            finally:
                canal.cerrar()
                self._set_salud(E.SALUD_KO, 'Engine sin respuesta')

    @staticmethod
    def _comprobar(canal):
        """Envía SALUD y devuelve la respuesta del Engine (OK / KO)."""
        respuesta = canal.peticion('SALUD')
        return respuesta if respuesta in (E.SALUD_OK, E.SALUD_KO) else E.SALUD_KO

    def iniciar(self):
        log(f"[MONITOR {self.ws_id}] Estación '{self.ubicacion}'")
        threading.Thread(target=self.bucle_central, daemon=True).start()
        self.bucle_engine()


if __name__ == '__main__':
    args = [a for a in sys.argv[1:] if a != '--registrar']
    auto_reg = '--registrar' in sys.argv
    if len(args) < 4:
        print('Uso: python WM_WS_M.py <puerto_engine> <ip_central> <puerto_central> <ws_id> [<ubicación>] [--registrar]')
        sys.exit(1)
    try:
        ubi = ' '.join(args[4:]) if len(args) >= 5 else 'Sin ubicación'
        Monitor(int(args[0]), args[1], int(args[2]), args[3], ubi, auto_registrar=auto_reg).iniciar()
    except KeyboardInterrupt:
        print('\n[MONITOR] Apagado.')
