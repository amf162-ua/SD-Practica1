import socket
import sys
import time
import threading

FORMAT = 'utf-8'
estado_salud = "OK"

def responder_ping(sock):
    global estado_salud
    try:
        while True:
            data = sock.recv(1024)
            if not data:
                break
            
            mensaje = data.decode(FORMAT).strip()
            if mensaje == "PING":
                sock.send(estado_salud.encode(FORMAT))
    except Exception:
        pass # La desconexión se gestionará en el bucle principal

def iniciar_engine(ip_monitor, puerto_monitor):
    global estado_salud
    
    # Hilo para cambiar estado por teclado entre OK y KO
    def leer_teclado():
        global estado_salud
        while True:
            try:
                entrada = input().strip().lower()
                if entrada == 'ko':
                    estado_salud = "KO"
                    print("\n[ALERTA ENGINE] Estado cambiado a 'KO' (Fuga/Avería).")
                elif entrada == 'ok':
                    estado_salud = "OK"
                    print("\n[INFO ENGINE] Estado restablecido a 'OK' (Sin averías).")
            except EOFError:
                break

    hilo_teclado = threading.Thread(target=leer_teclado, daemon=True)
    hilo_teclado.start()

    print("\n=== ENGINE ACTIVADO ===")
    print("Escribe 'ko' para simular avería/fuga.")
    print("Escribe 'ok' para reparar y volver a estado normal.")
    print("Presiona Ctrl+C para apagar.\n")

    # Bucle infinito de reconexión continua con el Monitor
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            print(f"[ENGINE] Intentando conectar con Monitor ({ip_monitor}:{puerto_monitor})...")
            sock.connect((ip_monitor, puerto_monitor))
            print("[ENGINE] Conectado con el Monitor.")
            
            # Gestiona la respuesta a los PINGs
            responder_ping(sock)
            print("[ENGINE] Conexión perdida con Monitor. Reintentando...")

        except (socket.error, ConnectionRefusedError):
            print("[ENGINE] Monitor no disponible. Reintentando en 3 segundos...")
        finally:
            sock.close()
            time.sleep(3)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python WM_WS_E.py <ip_monitor> <puerto_monitor>")
        sys.exit(1)

    ip_monitor = sys.argv[1]
    puerto_monitor = int(sys.argv[2])

    try:
        iniciar_engine(ip_monitor, puerto_monitor)
    except KeyboardInterrupt:
        print("\n[ENGINE] Apagado limpio por usuario (Ctrl+C).")
        sys.exit(0)