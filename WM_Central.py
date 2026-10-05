import socket
import sys
import threading
import json
import time
import tkinter as tk
from tkinter import ttk
from kafka import KafkaConsumer, KafkaProducer
from protocolo import empaquetar, desempaquetar, ACK, NACK, ENQ, EOT
from database import init_db, obtener_estaciones, guardar_o_actualizar_estacion

FORMAT = 'utf-8'

# Instancia global del panel de control
dashboard = None


class DashboardCentral:
    def __init__(self, root):
        self.root = root
        self.root.title("WaterManagement - Panel de Control Central")
        self.root.geometry("850x650")

        # Mapeo completo de estados a colores
        self.colores_estado = {
            "OK": "#2ecc71",             # Verde (Funcionamiento Normal)
            "CONECTADA": "#2ecc71",      # Verde (Estado equivalente)
            "REGANDO": "#27ae60",        # Verde Oscuro (Riego activo)
            "KO": "#e74c3c",             # Rojo (Fallo)
            "FUGA": "#e74c3c",           # Rojo (Fuga detectada)
            "AVERIA": "#e67e22",         # Naranja (Sin Engine)
            "AVERIADO": "#e67e22",       # Naranja
            "FUERA_SERVICIO": "#e67e22", # Naranja (Parada por Central)
            "DESCONECTADA": "#95a5a6"    # Gris (Sin socket)
        }

        self._crear_interfaz()
        self.cargar_estaciones_bd()

    def _crear_interfaz(self):
        # 1. Cabecera
        header = tk.Label(
            self.root, 
            text="MONITORIZACIÓN EN TIEMPO REAL DE ESTACIONES DE RIEGO", 
            font=("Helvetica", 11, "bold"), 
            bg="#2c3e50", 
            fg="white", 
            pady=8
        )
        header.pack(fill=tk.X)

        # 2. Tabla de Estaciones (Treeview)
        frame_tabla = ttk.Frame(self.root, padding=5)
        frame_tabla.pack(fill=tk.BOTH, expand=True)

        columnas = ("id", "ubicacion", "estado", "caudal")
        self.tree = ttk.Treeview(frame_tabla, columns=columnas, show="headings", height=6)

        self.tree.heading("id", text="ID Estación (WS)")
        self.tree.heading("ubicacion", text="Ubicación")
        self.tree.heading("estado", text="Estado")
        self.tree.heading("caudal", text="Caudal (L/min)")

        self.tree.column("id", width=120, anchor="center")
        self.tree.column("ubicacion", width=250, anchor="w")
        self.tree.column("estado", width=150, anchor="center")
        self.tree.column("caudal", width=120, anchor="center")

        scrollbar_tree = ttk.Scrollbar(frame_tabla, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=scrollbar_tree.set)
        
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar_tree.pack(side=tk.RIGHT, fill=tk.Y)

        for estado, color in self.colores_estado.items():
            self.tree.tag_configure(estado, background=color, foreground="white")

        # 3. Acciones de Central
        frame_acciones = ttk.LabelFrame(self.root, text=" Gestiones de Central ", padding=5)
        frame_acciones.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame_acciones, text="ID WS:").grid(row=0, column=0, padx=5, pady=2)
        self.entry_id = ttk.Entry(frame_acciones, width=12)
        self.entry_id.grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(frame_acciones, text="Ubicación:").grid(row=0, column=2, padx=5, pady=2)
        self.entry_ubicacion = ttk.Entry(frame_acciones, width=20)
        self.entry_ubicacion.grid(row=0, column=3, padx=5, pady=2)

        btn_alta = ttk.Button(frame_acciones, text="Registrar / Alta", command=self.registrar_estacion_manual)
        btn_alta.grid(row=0, column=4, padx=10, pady=2)

        # 4. Terminal / Consola
        frame_console = ttk.LabelFrame(self.root, text=" Registro de Eventos (Sockets & Kafka) ", padding=5)
        frame_console.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.txt_console = tk.Text(
            frame_console, 
            height=10, 
            bg="#1e1e1e", 
            fg="#dcdcdc", 
            insertbackground="white",
            font=("Consolas", 9)
        )
        scrollbar_console = ttk.Scrollbar(frame_console, orient=tk.VERTICAL, command=self.txt_console.yview)
        self.txt_console.configure(yscroll=scrollbar_console.set)

        self.txt_console.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar_console.pack(side=tk.RIGHT, fill=tk.Y)

        btn_limpiar = ttk.Button(frame_console, text="Limpiar Consola", command=self.limpiar_consola)
        btn_limpiar.pack(anchor=tk.E, pady=2)

        # 5. Barra de Estado
        self.lbl_resumen = tk.Label(
            self.root, 
            text="Total: 0 | Activas (OK/Regando): 0 | Incidencias/Avería: 0 | Desconectadas: 0", 
            bd=1, 
            relief=tk.SUNKEN, 
            anchor=tk.W, 
            padx=10,
            pady=4
        )
        self.lbl_resumen.pack(side=tk.BOTTOM, fill=tk.X)

    def log(self, mensaje):
        def _append():
            self.txt_console.insert(tk.END, mensaje + "\n")
            self.txt_console.see(tk.END)
        self.root.after(0, _append)

    def limpiar_consola(self):
        self.txt_console.delete("1.0", tk.END)

    def cargar_estaciones_bd(self):
        for item in self.tree.get_children():
            self.tree.delete(item)

        estaciones = obtener_estaciones()
        if estaciones:
            for est in estaciones:
                est_id = est[0]
                ubicacion = est[1]
                estado = est[2] if len(est) > 2 else "DESCONECTADA"
                self.tree.insert("", tk.END, iid=est_id, values=(est_id, ubicacion, estado, "0.0"), tags=(estado,))
        
        self.actualizar_contadores()

    def registrar_estacion_manual(self):
        ws_id = self.entry_id.get().strip()
        ubicacion = self.entry_ubicacion.get().strip()

        if ws_id and ubicacion:
            guardar_o_actualizar_estacion(ws_id, ubicacion=ubicacion, estado="DESCONECTADA")
            self.actualizar_estacion(ws_id, "DESCONECTADA", ubicacion=ubicacion)
            self.log(f"[CENTRAL] Estación {ws_id} registrada manualmente en BD.")
            self.entry_id.delete(0, tk.END)
            self.entry_ubicacion.delete(0, tk.END)

    def actualizar_estacion(self, ws_id, estado, ubicacion="Parque Central", caudal="0.0"):
        def _update():
            if self.tree.exists(ws_id):
                val_actuales = self.tree.item(ws_id, "values")
                ubi = ubicacion if ubicacion and ubicacion != "Parque Central" else val_actuales[1]
                self.tree.item(ws_id, values=(ws_id, ubi, estado, caudal), tags=(estado,))
            else:
                self.tree.insert("", tk.END, iid=ws_id, values=(ws_id, ubicacion, estado, caudal), tags=(estado,))
            
            self.actualizar_contadores()

        self.root.after(0, _update)

    def actualizar_contadores(self):
        items = self.tree.get_children()
        total = len(items)
        ok, ko, desconectadas = 0, 0, 0

        for item in items:
            tags = self.tree.item(item, "tags")
            if tags:
                estado = tags[0]
                if estado in ["OK", "CONECTADA", "REGANDO"]:
                    ok += 1
                elif estado in ["KO", "FUGA", "AVERIA", "AVERIADO", "FUERA_SERVICIO"]:
                    ko += 1
                elif estado == "DESCONECTADA":
                    desconectadas += 1

        self.lbl_resumen.config(
            text=f"Total: {total}  |  Activas (OK/Regando): {ok}  |  Incidencias/Avería: {ko}  |  Desconectadas: {desconectadas}"
        )


def log_central(texto):
    print(texto)
    if dashboard:
        dashboard.log(texto)


# --- SUBSISTEMA KAFKA EN CENTRAL ---

def iniciar_consumidor_kafka(bootstrap_server):
    """Hilo continuo que escucha órdenes y telemetría provenientes de Kafka."""
    log_central(f"[KAFKA] Conectando al broker en {bootstrap_server}...")
    
    consumer = None
    producer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                'wm-orders-request',
                'wm-telemetry',
                bootstrap_servers=bootstrap_server,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='central-group',
                auto_offset_reset='latest'
            )
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            log_central("[KAFKA] Conectado exitosamente a Kafka.")
        except Exception:
            time.sleep(3)

    for msg in consumer:
        topico = msg.topic
        datos = msg.value

        # 1. Solicitud Riego de Operario
        if topico == 'wm-orders-request':
            order_id = datos.get("order_id")
            ws_id = datos.get("ws_id")
            operator_id = datos.get("operator_id")
            
            log_central(f"[KAFKA] Solicitud de riego de {operator_id} para la estación {ws_id}")

            estaciones = obtener_estaciones()
            estado_actual = "DESCONECTADA"
            for est in estaciones:
                if est[0] == ws_id:
                    estado_actual = est[2] if len(est) > 2 else "DESCONECTADA"

            if estado_actual in ["OK", "CONECTADA"]:
                autorizado = True
                razon = "Estación disponible"
                guardar_o_actualizar_estacion(ws_id, estado="REGANDO")
                if dashboard:
                    dashboard.actualizar_estacion(ws_id, "REGANDO")
            else:
                autorizado = False
                razon = f"Estación no disponible (Estado actual: {estado_actual})"

            respuesta = {
                "order_id": order_id,
                "ws_id": ws_id,
                "operator_id": operator_id,
                "status": "AUTHORIZED" if autorizado else "DENIED",
                "reason": razon
            }
            producer.send('wm-orders-response', key=ws_id.encode('utf-8'), value=respuesta)
            log_central(f"[KAFKA] Respuesta enviada para {ws_id}: {respuesta['status']}")

        # 2. Telemetría del Engine en tiempo real
        elif topico == 'wm-telemetry':
            ws_id = datos.get("ws_id")
            caudal = str(datos.get("flow_rate_lmin", "0.0"))
            
            if dashboard:
                dashboard.actualizar_estacion(ws_id, "REGANDO", caudal=caudal)


# --- SUBSISTEMA SOCKETS (PRESENCIA HARDWARE MONITORES) ---

def atender_monitor(conn, addr):
    log_central(f"[NUEVA CONEXIÓN] Monitor conectado desde {addr}")
    ws_id = None
    try:
        while True:
            trama = conn.recv(1024)
            if not trama:
                break
                
            if trama == ENQ:
                conn.send(ACK)
                continue
            if trama == EOT:
                break

            mensaje, valido = desempaquetar(trama)
            if not valido:
                conn.send(NACK)
                continue
            else:
                conn.send(ACK)

            log_central(f"[SOCKETS] Mensaje de {addr}: {mensaje}")
            
            if mensaje.startswith("AUTH#"):
                partes = mensaje.split("#")
                ws_id = partes[1]
                estado_inicial = partes[2] if len(partes) > 2 else "AVERIA"
                
                guardar_o_actualizar_estacion(ws_id, ubicacion="Parque Central", estado=estado_inicial)
                if dashboard:
                    dashboard.actualizar_estacion(ws_id, estado_inicial)

                conn.send(empaquetar("OK#AUTENTICADO"))
                conn.recv(1024)
                
            elif mensaje.startswith("STATUS#") or mensaje.startswith("STATE#"):
                partes = mensaje.split("#")
                ws_id = partes[1]
                nuevo_estado = partes[2]
                caudal = partes[3] if len(partes) > 3 else "0.0"

                guardar_o_actualizar_estacion(ws_id, estado=nuevo_estado)
                if dashboard:
                    dashboard.actualizar_estacion(ws_id, estado=nuevo_estado, caudal=caudal)
                
                conn.send(empaquetar("OK#ESTADO_RECIBIDO"))
                conn.recv(1024)

            elif mensaje.startswith("LEAK#"):
                partes = mensaje.split("#")
                ws_id = partes[1]
                
                guardar_o_actualizar_estacion(ws_id, estado="FUGA")
                if dashboard:
                    dashboard.actualizar_estacion(ws_id, "FUGA")

                log_central(f"[SOCKETS] ¡ALERTA! Fuga registrada en la estación {ws_id}")
                conn.send(empaquetar("OK#FUGA_REGISTRADA"))
                conn.recv(1024)

    except Exception as e:
        log_central(f"[ERROR SOCKET] {addr}: {e}")
    finally:
        if ws_id:
            guardar_o_actualizar_estacion(ws_id, estado="DESCONECTADA")
            if dashboard:
                dashboard.actualizar_estacion(ws_id, "DESCONECTADA")
        conn.close()


def iniciar_servidor_sockets(puerto):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', puerto))
    server.listen()
    server.settimeout(1.0)

    log_central(f"[CENTRAL ACTIVADA] Escuchando sockets en el puerto {puerto}...")

    while True:
        try:
            conn, addr = server.accept()
            conn.settimeout(None)
            thread = threading.Thread(target=atender_monitor, args=(conn, addr), daemon=True)
            thread.start()
        except socket.timeout:
            continue
        except Exception:
            break

    server.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python WM_Central.py <puerto_sockets> [<ip_kafka> <puerto_kafka>]")
        sys.exit(1)
        
    puerto_escucha = int(sys.argv[1])
    ip_kafka = sys.argv[2] if len(sys.argv) > 2 else "localhost"
    puerto_kafka = sys.argv[3] if len(sys.argv) > 3 else "9092"
    bootstrap_kafka = f"{ip_kafka}:{puerto_kafka}"

    init_db()

    # 1. Hilo Servidor de Sockets
    hilo_sockets = threading.Thread(
        target=iniciar_servidor_sockets, 
        args=(puerto_escucha,), 
        daemon=True
    )
    hilo_sockets.start()

    # 2. Hilo Consumidor/Productor Kafka
    hilo_kafka = threading.Thread(
        target=iniciar_consumidor_kafka, 
        args=(bootstrap_kafka,), 
        daemon=True
    )
    hilo_kafka.start()

    # 3. GUI Principal Tkinter
    root = tk.Tk()
    dashboard = DashboardCentral(root)
    
    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("\n[CENTRAL] Apagado limpio completado.")
        sys.exit(0)