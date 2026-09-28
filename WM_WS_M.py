import socket
import sys
import time
import threading
from protocolo import empaquetar, desempaquetar, ACK, NACK, ENQ, EOT

FORMAT = 'utf-8'

# Variables globales para compartir la conexión a Central entre hilos
sock_central_global = None
lock_central = threading.Lock()

# Estado global del Monitor respecto a la Engine
# Valores posibles: "AVERIA" (Sin Engine), "OK" (Engine OK), "KO" (Engine KO / Fuga)
estado_actual_monitor = "AVERIA"
engine_conectado = False


def enviar_a_central(mensaje_str):
    """Función auxiliar para enviar tramas a la Central siguiendo el protocolo."""
    global sock_central_global
    with lock_central:
        if sock_central_global is None:
            return False
        try:
            # 1. Iniciar transmisión
            sock_central_global.send(ENQ)
            if sock_central_global.recv(1024) != ACK:
                return False
            
            # 2. Enviar trama empaquetada
            sock_central_global.send(empaquetar(mensaje_str))
            if sock_central_global.recv(1024) != ACK:
                return False
            
            # 3. Recibir respuesta
            trama_resp = sock_central_global.recv(1024)
            resp_msg, valido = desempaquetar(trama_resp)
            
            # 4. Asentir la respuesta
            if valido:
                sock_central_global.send(ACK)
                return resp_msg
            else:
                sock_central_global.send(NACK)
                return None
        except Exception as e:
            print(f"[ERROR MONITOR] Fallo al comunicar con Central: {e}")
            return None


def mantener_conexion_central(ip_central, puerto_central, ws_id):
    """Mantiene el socket hacia la Central y envía el estado actual periódicamente."""
    global sock_central_global, estado_actual_monitor, engine_conectado

    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            print(f"[MONITOR] Conectando a CENTRAL ({ip_central}:{puerto_central})...")
            sock.connect((ip_central, puerto_central))
            
            with lock_central:
                sock_central_global = sock
            
            # Autenticación inicial con estado 'AVERIA' si no hay Engine conectado
            estado_inicial = estado_actual_monitor
            respuesta = enviar_a_central(f"AUTH#{ws_id}#{estado_inicial}")
            if respuesta:
                print(f"[MONITOR <- CENTRAL] Autenticado: {respuesta}")
            
            # Bucle continuo para reportar cambios de estado a la Central
            ultimo_estado_enviado = None

            while True:
                # Si no hay Engine, nos aseguramos de que el estado sea 'AVERIA'
                if not engine_conectado:
                    estado_actual_monitor = "AVERIA"

                # Si cambia el estado o de forma periódica, notificamos a Central
                if estado_actual_monitor != ultimo_estado_enviado:
                    msg = f"STATUS#{ws_id}#{estado_actual_monitor}#0.0"
                    res = enviar_a_central(msg)
                    if res:
                        ultimo_estado_enviado = estado_actual_monitor
                        print(f"[MONITOR -> CENTRAL] Estado actualizado a: {estado_actual_monitor}")

                time.sleep(1)

        except (socket.error, ConnectionRefusedError):
            print("[MONITOR] CENTRAL no disponible. Reintentando en 3 segundos...")
        finally:
            with lock_central:
                sock_central_global = None
            sock.close()
            time.sleep(3)


def escuchar_engine(puerto_local, ws_id):
    """Servidor local para aceptar la conexión del Engine."""
    global estado_actual_monitor, engine_conectado

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', puerto_local))
    server.listen(1)
    server.settimeout(1.0)
    
    print(f"[MONITOR] Servidor interno listo en puerto {puerto_local}. Esperando Engine...")

    while True:
        try:
            conn_engine, addr = server.accept()
            conn_engine.settimeout(None)
            engine_conectado = True
            print(f"\n[MONITOR] Engine conectado desde {addr}")

            try:
                while True:
                    time.sleep(1)
                    
                    # Protocolo PING a Engine
                    conn_engine.send(ENQ)
                    if conn_engine.recv(1024) == ACK:
                        conn_engine.send(empaquetar("PING"))
                        
                        if conn_engine.recv(1024) == ACK:
                            trama_resp = conn_engine.recv(1024)
                            respuesta, valido = desempaquetar(trama_resp)
                            
                            if valido:
                                conn_engine.send(ACK)
                                # Actualizar estado del Monitor según respuesta del Engine
                                estado_actual_monitor = respuesta  # "OK" o "KO"
                                print(f"[MONITOR <- ENGINE] Estado de salud: {respuesta}")
                            else:
                                conn_engine.send(NACK)

            except (socket.error, ConnectionResetError):
                print("[MONITOR] Conexión perdida con el Engine.")
            finally:
                engine_conectado = False
                estado_actual_monitor = "AVERIA"
                conn_engine.close()

        except socket.timeout:
            continue


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Uso: python WM_WS_M.py <puerto_servidor_engine> <ip_central> <puerto_central> <ws_id>")
        sys.exit(1)

    puerto_local = int(sys.argv[1])
    ip_central = sys.argv[2]
    puerto_central = int(sys.argv[3])
    ws_id = sys.argv[4]

    try:
        # Hilo persistente para comunicación con Central
        hilo_central = threading.Thread(
            target=mantener_conexion_central, 
            args=(ip_central, puerto_central, ws_id), 
            daemon=True
        )
        hilo_central.start()
        
        # Bucle principal escuchando al Engine
        escuchar_engine(puerto_local, ws_id)
        
    except KeyboardInterrupt:
        print("\n[MONITOR] Apagado limpio por usuario (Ctrl+C).")
        sys.exit(0)