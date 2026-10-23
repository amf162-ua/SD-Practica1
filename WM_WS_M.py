import socket
import sys
import time
import threading
from protocolo import empaquetar, desempaquetar, ACK, NACK, ENQ, EOT

class WateringStationMonitor:
    def __init__(self, puerto_engine: int, ip_central: str, puerto_central: int, ws_id: str):
        self.puerto_engine = puerto_engine
        self.ip_central = ip_central
        self.puerto_central = puerto_central
        self.ws_id = ws_id

        # Sockets e hilos
        self.sock_central = None
        self.lock_central = threading.Lock()
        
        # Estados internos
        self.estado_actual = "AVERIA"  # Estado inicial sin Engine
        self.engine_conectado = False
        self.ejecutando = True

    def enviar_a_central(self, mensaje_str: str) -> str:
        """Envía un mensaje a Central usando el protocolo de enlace (ENQ/ACK -> Trama -> ACK)."""
        with self.lock_central:
            if self.sock_central is None:
                return None
            try:
                # 1. Petición de transmisión
                self.sock_central.send(ENQ)
                if self.sock_central.recv(1024) != ACK:
                    return None

                # 2. Envío de trama empaquetada
                self.sock_central.send(empaquetar(mensaje_str))
                if self.sock_central.recv(1024) != ACK:
                    return None

                # 3. Recepción de respuesta de Central
                trama_resp = self.sock_central.recv(1024)
                resp_msg, valido = desempaquetar(trama_resp)

                # 4. Asentimiento final
                if valido:
                    self.sock_central.send(ACK)
                    return resp_msg
                else:
                    self.sock_central.send(NACK)
                    return None
            except Exception as e:
                print(f"[MONITOR {self.ws_id}] Error en comunicación con Central: {e}")
                return None

    def hilo_comunicacion_central(self):
        """Mantiene la conexión socket persistente con Central y reporta cambios de estado."""
        while self.ejecutando:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                print(f"[MONITOR {self.ws_id}] Conectando a Central en {self.ip_central}:{self.puerto_central}...")
                sock.connect((self.ip_central, self.puerto_central))

                with self.lock_central:
                    self.sock_central = sock

                # Autenticación inicial
                res_auth = self.enviar_a_central(f"AUTH#{self.ws_id}#{self.estado_actual}")
                if res_auth:
                    print(f"[MONITOR {self.ws_id}] Autenticado en Central: {res_auth}")

                ultimo_estado_enviado = None

                while self.ejecutando:
                    # Si el Engine se desconectó, aseguramos estado 'AVERIA'
                    if not self.engine_conectado:
                        self.estado_actual = "AVERIA"

                    # Reportar a Central si hay cambio de estado
                    if self.estado_actual != ultimo_estado_enviado:
                        if self.estado_actual == "KO":
                            msg = f"LEAK#{self.ws_id}"
                        else:
                            msg = f"STATUS#{self.ws_id}#{self.estado_actual}#0.0"

                        respuesta = self.enviar_a_central(msg)
                        if respuesta:
                            ultimo_estado_enviado = self.estado_actual
                            print(f"[MONITOR {self.ws_id}] Estado actualizado en Central: {self.estado_actual}")

                    time.sleep(1)

            except (socket.error, ConnectionRefusedError):
                print(f"[MONITOR {self.ws_id}] Central no disponible. Reintentando en 3s...")
            finally:
                with self.lock_central:
                    self.sock_central = None
                sock.close()
                time.sleep(3)

    def escuchar_engine(self):
        """Servidor TCP local para gestionar la conexión del Engine."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', self.puerto_engine))
        server.listen(1)
        server.settimeout(1.0)

        print(f"[MONITOR {self.ws_id}] Servidor listo en puerto {self.puerto_engine}. Esperando Engine...")

        while self.ejecutando:
            try:
                conn_engine, addr = server.accept()
                conn_engine.settimeout(None)
                self.engine_conectado = True
                print(f"[MONITOR {self.ws_id}] Engine conectado desde {addr}")

                try:
                    # Configuración inicial del Engine
                    conn_engine.send(ENQ)
                    if conn_engine.recv(1024) == ACK:
                        conn_engine.send(empaquetar(f"CONFIG#{self.ws_id}"))
                        if conn_engine.recv(1024) == ACK:
                            trama_conf = conn_engine.recv(1024)
                            conf_resp, valido = desempaquetar(trama_conf)
                            if valido:
                                conn_engine.send(ACK)
                                print(f"[MONITOR {self.ws_id}] ID {self.ws_id} asignado al Engine")

                    # Bucle PING a Engine
                    while self.ejecutando:
                        time.sleep(1)
                        conn_engine.send(ENQ)
                        ack = conn_engine.recv(1024)
                        if ack and ack.startswith(ACK):
                            conn_engine.send(empaquetar("PING"))
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

                except (socket.error, ConnectionResetError):
                    print(f"[MONITOR {self.ws_id}] Conexión perdida con el Engine.")
                finally:
                    self.engine_conectado = False
                    self.estado_actual = "AVERIA"
                    conn_engine.close()

            except socket.timeout:
                continue

    def iniciar(self):
        """Inicia los hilos de ejecución del Monitor."""
        hilo_central = threading.Thread(target=self.hilo_comunicacion_central, daemon=True)
        hilo_central.start()
        self.escuchar_engine()


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Uso: python WM_WS_M.py <puerto_servidor_engine> <ip_central> <puerto_central> <ws_id>")
        sys.exit(1)

    puerto_local = int(sys.argv[1])
    ip_central = sys.argv[2]
    puerto_central = int(sys.argv[3])
    ws_id = sys.argv[4]

    monitor = WateringStationMonitor(puerto_local, ip_central, puerto_central, ws_id)
    try:
        monitor.iniciar()
    except KeyboardInterrupt:
        print(f"\n[MONITOR {ws_id}] Apagado ordenado.")
        sys.exit(0)