import socket
import sys
import threading
from database import init_db, obtener_estaciones, guardar_o_actualizar_estacion

FORMAT = 'utf-8'

def atender_monitor(conn, addr):
    print(f"[NUEVA CONEXIÓN] Monitor conectado desde {addr}")
    ws_id = None
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            
            mensaje = data.decode(FORMAT).strip()
            print(f"[RECEPTOR CENTRAL] Recibido de {addr}: {mensaje}")
            
            if mensaje.startswith("AUTH#"):
                partes = mensaje.split("#")
                ws_id = partes[1]
                guardar_o_actualizar_estacion(ws_id, ubicacion="Parque Central", estado="CONECTADA")
                respuesta = "OK#AUTENTICADO"
                conn.send(respuesta.encode(FORMAT))
                
    except Exception as e:
        print(f"[ERROR CLIENTE] {addr}: {e}")
    finally:
        if ws_id:
            guardar_o_actualizar_estacion(ws_id, estado="DESCONECTADA")
        conn.close()
        print(f"[CONEXIÓN CERRADA] Monitor {addr}")

def iniciar_central(puerto):
    init_db()
    
    estaciones = obtener_estaciones()
    print("=== ESTADO INICIAL DE ESTACIONES EN BD ===")
    if not estaciones:
        print("No hay estaciones registradas en BD.")
    else:
        for est_id, ubicacion in estaciones:
            print(f"ID: {est_id} | Ubicación: {ubicacion}")
    print("=========================================\n")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Permite reutilizar el puerto inmediatamente si se reinicia rápido
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', puerto))
    server.listen()
    server.settimeout(1.0)

    print(f"[CENTRAL ACTIVADA] Escuchando en puerto {puerto}...")

    try:
        while True:
            try:
                conn, addr = server.accept()
                conn.settimeout(None)
                
                # Hilo en modo demonio (daemon=True) para que se destruya al apagar
                thread = threading.Thread(target=atender_monitor, args=(conn, addr), daemon=True)
                thread.start()
            except socket.timeout:
                continue
    finally:
        server.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python WM_Central.py <puerto_escucha>")
        sys.exit(1)
        
    puerto_escucha = int(sys.argv[1])
    try:
        iniciar_central(puerto_escucha)
    except KeyboardInterrupt:
        print("\n[CENTRAL] Apagado limpio completado.")
        sys.exit(0)