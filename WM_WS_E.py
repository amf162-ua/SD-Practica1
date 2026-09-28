import socket
import sys
import time
import threading
from protocolo import empaquetar, desempaquetar, ACK, NACK, ENQ, EOT

FORMAT = 'utf-8'
estado_salud = "OK"

def responder_ping(sock):
    global estado_salud
    try:
        while True:
            trama = sock.recv(1024)
            if not trama:
                break
            
            # El Monitor inicia con ENQ
            if trama == ENQ:
                sock.send(ACK)
                continue
            elif trama == EOT:
                break
            
            # Desempaquetar el PING
            mensaje, valido = desempaquetar(trama)
            if not valido:
                sock.send(NACK)
                continue
            else:
                sock.send(ACK)
            
            if mensaje == "PING":
                # Enviar estado actual empaquetado (OK o KO)
                respuesta = empaquetar(estado_salud)
                sock.send(respuesta)
                
                # Esperar el ACK del Monitor confirmando recepción
                ack_recibido = sock.recv(1024)
                if ack_recibido != ACK:
                    print("[ENGINE] Advertencia: Monitor no confirmó la recepción del estado.")
                    
    except Exception:
        pass # La reconexión se gestiona en el bucle principal

def iniciar_engine(ip_monitor, puerto_monitor):
    global estado_salud
    
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

    # Hilo en segundo plano para escuchar el teclado sin bloquear los sockets
    threading.Thread(target=leer_teclado, daemon=True).start()

    print("\n=== ENGINE ACTIVADO ===")
    print("Escribe 'ko' para simular avería/fuga.")
    print("Escribe 'ok' para reparar y volver a estado normal.")
    print("Presiona Ctrl+C para apagar.\n")

    # Bucle infinito de conexión persistente con el Monitor
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            print(f"[ENGINE] Conectando con Monitor ({ip_monitor}:{puerto_monitor})...")
            sock.connect((ip_monitor, puerto_monitor))
            print("[ENGINE] Conectado con el Monitor.")
            
            responder_ping(sock)
            print("[ENGINE] Conexión perdida con Monitor. Reintentando...")

        except (socket.error, ConnectionRefusedError):
            print("[ENGINE] Monitor no disponible. Reintentando en 3 segundos...")
        finally:
            sock.close()
            time.sleep(3)

if __name__ == "__main__":
    # Verificación de parámetros según la especificación del sistema
    if len(sys.argv) < 5:
        print("Uso: python WM_WS_E.py <ip_broker_kafka> <puerto_broker> <ip_monitor> <puerto_monitor>")
        sys.exit(1)

    # Parámetros exigidos por la especificación (preparados para la futura fase de eventos)
    ip_kafka = sys.argv[1]
    puerto_kafka = sys.argv[2]
    ip_monitor = sys.argv[3]
    puerto_monitor = int(sys.argv[4])

    try:
        iniciar_engine(ip_monitor, puerto_monitor)
    except KeyboardInterrupt:
        print("\n[ENGINE] Apagado limpio por usuario (Ctrl+C).")
        sys.exit(0)