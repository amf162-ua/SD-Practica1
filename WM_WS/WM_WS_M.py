import socket
import sys
import time
import threading
import os

DIRECTORIO_ACTUAL = os.path.dirname(os.path.abspath(__file__))
if DIRECTORIO_ACTUAL not in sys.path:
    sys.path.insert(0, DIRECTORIO_ACTUAL)

from protocolo import empaquetar, desempaquetar, ACK, NACK, ENQ, EOT


class WateringStationMonitor:
    """
    Módulo Monitor de la Estación de Riego (WM_WS_M).

    Responsabilidades:
    - Conectarse a Central (WM_Central) mediante socket TCP persistente.
    - Autenticarse enviando AUTH con ID y ubicación de la estación.
    - Reportar cambios de estado a Central (STATUS / LEAK).
    - Aceptar la conexión del Engine (WM_WS_E) y enviarle PINGs periódicos de salud.
    - Propagar KO del Engine a Central como notificación de FUGA.
    """

    def __init__(self, puerto_engine: int, ip_central: str, puerto_central: int,
                 ws_id: str, ubicacion: str = "Parque Central"):
        self.puerto_engine  = puerto_engine
        self.ip_central     = ip_central
        self.puerto_central = puerto_central
        self.ws_id          = ws_id
        self.ubicacion      = ubicacion

        self.sock_central      = None
        self.lock_central      = threading.Lock()

        # Estado de la estación tal como lo percibe el Monitor
        # Comienza en AVERIA porque el Engine aún no está conectado
        self.estado_actual   = "AVERIA"
        self.engine_conectado = False
        self.ejecutando       = True

    def enviar_a_central(self, mensaje_str: str) -> str:
        """
        Envía un mensaje a Central siguiendo el protocolo:
          1. Monitor → ENQ        → Central
          2. Central → ACK        → Monitor
          3. Monitor → <trama>    → Central
          4. Central → ACK/NACK   → Monitor   (confirmación de recepción de trama)
          5. Central → <respuesta>→ Monitor
          6. Monitor → ACK        → Central   (confirmación de recepción de respuesta)

        Devuelve el mensaje de respuesta de Central, o None si algo falla.
        """
        with self.lock_central:
            if self.sock_central is None:
                return None
            try:
                # 1-2: Handshake ENQ/ACK
                self.sock_central.send(ENQ)
                resp_enq = self.sock_central.recv(1024)
                if resp_enq != ACK:
                    return None

                # 3-4: Enviar trama y recibir ACK de recepción
                self.sock_central.send(empaquetar(mensaje_str))
                ack_trama = self.sock_central.recv(1024)
                if ack_trama != ACK:
                    return None  # Central indicó error de LRC (NACK)

                # 5: Recibir trama de respuesta de Central
                trama_resp = self.sock_central.recv(1024)
                if not trama_resp:
                    return None

                resp_msg, valido = desempaquetar(trama_resp)

                # 6: Enviar ACK o NACK a Central según validez de la respuesta
                self.sock_central.send(ACK if valido else NACK)

                return resp_msg if valido else None

            except Exception as e:
                print(f"[MONITOR {self.ws_id}] Error en comunicación con Central: {e}")
                return None

    def hilo_comunicacion_central(self):
        """
        Mantiene una conexión TCP persistente con Central.
        En caso de pérdida de conexión, reintenta automáticamente cada 3 segundos.
        """
        while self.ejecutando:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                print(f"[MONITOR {self.ws_id}] Conectando a Central ({self.ip_central}:{self.puerto_central})...")
                sock.connect((self.ip_central, self.puerto_central))

                with self.lock_central:
                    self.sock_central = sock

                # Autenticación inicial: envía ID y ubicación
                res_auth = self.enviar_a_central(f"AUTH#{self.ws_id}#{self.ubicacion}#{self.estado_actual}")
                if res_auth:
                    print(f"[MONITOR {self.ws_id}] Autenticado en Central: {res_auth}")
                else:
                    print(f"[MONITOR {self.ws_id}] Autenticación fallida. Reintentando...")
                    raise ConnectionError("Auth fallida")

                ultimo_estado_enviado = None

                while self.ejecutando:
                    # Si el Engine no está conectado, el estado es AVERIA
                    if not self.engine_conectado:
                        self.estado_actual = "AVERIA"

                    # Solo enviar si el estado cambió
                    if self.estado_actual != ultimo_estado_enviado:
                        if self.estado_actual == "KO":
                            # KO → fuga detectada → enviar LEAK
                            msg = f"LEAK#{self.ws_id}"
                        else:
                            msg = f"STATUS#{self.ws_id}#{self.estado_actual}"

                        respuesta = self.enviar_a_central(msg)
                        if respuesta:
                            ultimo_estado_enviado = self.estado_actual
                            print(f"[MONITOR {self.ws_id}] Estado notificado a Central: {self.estado_actual}")
                        else:
                            print(f"[MONITOR {self.ws_id}] No se pudo notificar estado. Reconectando...")
                            break

                    time.sleep(1)

            except (socket.error, ConnectionRefusedError, ConnectionError) as e:
                print(f"[MONITOR {self.ws_id}] Central no disponible ({e}). Reintentando en 3s...")
            finally:
                with self.lock_central:
                    self.sock_central = None
                sock.close()
                time.sleep(3)

    def escuchar_engine(self):
        """
        Servidor TCP local que espera la conexión del Engine.
        Una vez conectado, envía PINGs periódicos cada segundo.
        La respuesta del Engine ('OK' / 'KO') actualiza el estado de la estación.
        """
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', self.puerto_engine))
        server.listen(1)
        server.settimeout(1.0)

        print(f"[MONITOR {self.ws_id}] Esperando conexión del Engine en puerto {self.puerto_engine}...")

        while self.ejecutando:
            try:
                conn_engine, addr = server.accept()
                conn_engine.settimeout(None)
                self.engine_conectado = True
                print(f"[MONITOR {self.ws_id}] Engine conectado desde {addr}")

                try:
                    # ── Configuración inicial: asignar ID al Engine ──
                    conn_engine.send(ENQ)
                    if conn_engine.recv(1024) == ACK:
                        conn_engine.send(empaquetar(f"CONFIG#{self.ws_id}"))
                        ack_cfg = conn_engine.recv(1024)
                        if ack_cfg == ACK:
                            trama_conf = conn_engine.recv(1024)
                            conf_resp, valido = desempaquetar(trama_conf)
                            if valido:
                                conn_engine.send(ACK)
                                print(f"[MONITOR {self.ws_id}] Engine configurado con ID {self.ws_id}")

                    # ── Bucle de PING de salud (cada 1 segundo) ──
                    while self.ejecutando:
                        time.sleep(1)

                        # ENQ → ACK
                        conn_engine.send(ENQ)
                        ack = conn_engine.recv(1024)
                        if not ack or not ack.startswith(ACK):
                            break

                        # Enviar PING
                        conn_engine.send(empaquetar("PING"))

                        # Recibir respuesta (manejar coalescencia TCP: ACK+trama juntos)
                        trama_resp = conn_engine.recv(1024)
                        if trama_resp.startswith(ACK):
                            trama_resp = trama_resp[1:]
                            if not trama_resp:
                                trama_resp = conn_engine.recv(1024)

                        respuesta, valido = desempaquetar(trama_resp)

                        if valido:
                            conn_engine.send(ACK)
                            self.estado_actual = respuesta  # 'OK' o 'KO'
                        else:
                            conn_engine.send(NACK)

                except (socket.error, ConnectionResetError) as e:
                    print(f"[MONITOR {self.ws_id}] Conexión perdida con el Engine: {e}")
                finally:
                    self.engine_conectado = False
                    self.estado_actual = "AVERIA"
                    conn_engine.close()
                    print(f"[MONITOR {self.ws_id}] Engine desconectado. Estado → AVERIA.")

            except socket.timeout:
                continue

        server.close()

    def iniciar(self):
        """Arranca el Monitor: hilo de comunicación con Central + servidor para el Engine."""
        threading.Thread(target=self.hilo_comunicacion_central, daemon=True).start()
        self.escuchar_engine()  # Bloquea en el hilo principal (bucle de escucha)


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Uso: python WM_WS_M.py <puerto_engine> <ip_central> <puerto_central> <ws_id> [<ubicacion>]")
        print("  Ejemplo: python WM_WS_M.py 6001 192.168.1.10 5000 WS_01 'Parque Norte'")
        sys.exit(1)

    puerto_local  = int(sys.argv[1])
    ip_central    = sys.argv[2]
    puerto_central = int(sys.argv[3])
    ws_id         = sys.argv[4]
    ubicacion     = " ".join(sys.argv[5:]) if len(sys.argv) > 5 else "Parque Central"

    monitor = WateringStationMonitor(puerto_local, ip_central, puerto_central, ws_id, ubicacion)
    try:
        monitor.iniciar()
    except KeyboardInterrupt:
        print(f"\n[MONITOR {ws_id}] Apagado ordenado.")
        sys.exit(0)