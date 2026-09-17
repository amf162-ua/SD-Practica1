import socket
import sys
import time
import threading

FORMAT = 'utf-8'

def mantener_conexion_central(ip_central, puerto_central, ws_id):
    """
    Mantiene un socket TCP persistente con CENTRAL.
    Si se cae la red o Central se apaga, reintenta automáticamente.
    """
    while True:
        sock_central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            print(f"[MONITOR] Conectando a CENTRAL ({ip_central}:{puerto_central})...")
            sock_central.connect((ip_central, puerto_central))
            
            # Autenticación inicial
            mensaje_auth = f"AUTH#{ws_id}"
            sock_central.send(mensaje_auth.encode(FORMAT))
            
            respuesta = sock_central.recv(1024).decode(FORMAT)
            print(f"[MONITOR <- CENTRAL] Respuesta: {respuesta}")
            
            # Mantiene el socket abierto en bucle.
            # Si Central cierra la conexión o cae, recv() retornará vacío/error
            while True:
                data = sock_central.recv(1024)
                if not data:
                    print("[MONITOR] Central ha cerrado la conexión.")
                    break
                    
        except (socket.error, ConnectionRefusedError):
            print("[MONITOR] CENTRAL no disponible. Reintentando en 3 segundos...")
        finally:
            sock_central.close()
            time.sleep(3)

def escuchar_engine(puerto_local):
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
            print(f"\n[MONITOR] Engine conectado desde {addr}")

            try:
                while True:
                    time.sleep(1)
                    conn_engine.send("PING".encode(FORMAT))
                    
                    respuesta = conn_engine.recv(1024).decode(FORMAT).strip()
                    if not respuesta:
                        print("[MONITOR] Engine desconectado.")
                        break
                        
                    print(f"[MONITOR -> ENGINE] Salud: {respuesta}")

                    if respuesta == "KO":
                        print("[ALERT MONITOR] Fuga/Avería reportada por Engine!")

            except (socket.error, ConnectionResetError):
                print("[MONITOR] Conexión perdida con el Engine. Esperando reconexión...")
            finally:
                conn_engine.close()

        except socket.timeout:
            continue

def iniciar_monitor(puerto_local, ip_central, puerto_central, ws_id):
    # 1. Hilo persistente para la conexión con Central (Daemon para responder a Ctrl+C)
    hilo_central = threading.Thread(
        target=mantener_conexion_central, 
        args=(ip_central, puerto_central, ws_id), 
        daemon=True
    )
    hilo_central.start()
    
    # 2. Bucle continuo de escucha para Engine en el hilo principal
    escuchar_engine(puerto_local)

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Uso: python WM_WS_M.py <puerto_servidor_engine> <ip_central> <puerto_central> <ws_id>")
        sys.exit(1)

    puerto_local = int(sys.argv[1])
    ip_central = sys.argv[2]
    puerto_central = int(sys.argv[3])
    ws_id = sys.argv[4]

    try:
        iniciar_monitor(puerto_local, ip_central, puerto_central, ws_id)
    except KeyboardInterrupt:
        print("\n[MONITOR] Apagado limpio por usuario (Ctrl+C).")
        sys.exit(0)