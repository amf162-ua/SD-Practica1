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

def deserializar_json(mensaje_bytes):
    """Convierte los bytes recibidos de Kafka en un diccionario JSON."""
    if mensaje_bytes is None:
        return None
    try:
        return json.loads(mensaje_bytes.decode('utf-8'))
    except Exception:
        return None


class DashboardCentral:
    def __init__(self, root, central_app):
        self.root = root
        self.central_app = central_app
        self.root.title("WaterManagement — Panel de Control Central")
        self.root.geometry("950x700")

        # Colores de fondo según especificación del PDF (Página 4)
        self.colores_estado = {
            "OK": "#2ecc71",             # Verde (Disponible / Activada)
            "CONECTADA": "#2ecc71",      # Verde
            "REGANDO_A": "#27ae60",     # Verde Oscuro (Parpadeo Fase 1)
            "REGANDO_B": "#2ecc71",     # Verde Claro (Parpadeo Fase 2)
            "FUGA": "#e74c3c",           # Rojo (Fuga detectada)
            "KO": "#e74c3c",             # Rojo
            "AVERIA": "#e67e22",         # Naranja (Sin Engine)
            "FUERA_SERVICIO": "#e67e22", # Naranja (Parada por Central)
            "DESCONECTADA": "#95a5a6"    # Gris (Sin socket)
        }

        self.fase_parpadeo = False

        self._crear_interfaz()
        self.cargar_estaciones_bd()
        self._iniciar_bucle_parpadeo()

    def _crear_interfaz(self):
        # 1. Cabecera
        header = tk.Label(
            self.root, 
            text="SISTEMA CENTRAL DE MONITORIZACIÓN Y CONTROL (WaterManagement)", 
            font=("Helvetica", 11, "bold"), 
            bg="#2c3e50", 
            fg="white", 
            pady=8
        )
        header.pack(fill=tk.X)

        # 2. Tabla de Estaciones (Treeview)
        frame_tabla = ttk.Frame(self.root, padding=5)
        frame_tabla.pack(fill=tk.BOTH, expand=True)

        columnas = ("id", "ubicacion", "estado", "caudal", "volumen", "operario")
        self.tree = ttk.Treeview(frame_tabla, columns=columnas, show="headings", height=8)

        self.tree.heading("id", text="ID Estación")
        self.tree.heading("ubicacion", text="Ubicación")
        self.tree.heading("estado", text="Estado")
        self.tree.heading("caudal", text="Caudal (L/min)")
        self.tree.heading("volumen", text="Vol. Acum. (L)")
        self.tree.heading("operario", text="Operario")

        self.tree.column("id", width=100, anchor="center")
        self.tree.column("ubicacion", width=220, anchor="w")
        self.tree.column("estado", width=140, anchor="center")
        self.tree.column("caudal", width=100, anchor="center")
        self.tree.column("volumen", width=110, anchor="center")
        self.tree.column("operario", width=120, anchor="center")

        scrollbar_tree = ttk.Scrollbar(frame_tabla, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=scrollbar_tree.set)
        
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar_tree.pack(side=tk.RIGHT, fill=tk.Y)

        for estado, color in self.colores_estado.items():
            self.tree.tag_configure(estado, background=color, foreground="white")

        # 3. Panel de Acciones y Control Arbitrario (Punto 11 del PDF)
        frame_acciones = ttk.LabelFrame(self.root, text=" Gestiones y Órdenes de Central (Punto 11) ", padding=5)
        frame_acciones.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame_acciones, text="ID WS:").grid(row=0, column=0, padx=5, pady=2)
        self.entry_id = ttk.Entry(frame_acciones, width=12)
        self.entry_id.grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(frame_acciones, text="Ubicación:").grid(row=0, column=2, padx=5, pady=2)
        self.entry_ubicacion = ttk.Entry(frame_acciones, width=20)
        self.entry_ubicacion.grid(row=0, column=3, padx=5, pady=2)

        btn_alta = ttk.Button(frame_acciones, text="Alta / Registro BD", command=self.registrar_estacion_manual)
        btn_alta.grid(row=0, column=4, padx=5, pady=2)

        # Botones para enviar órdenes arbitrarias a las WS
        btn_iniciar = ttk.Button(frame_acciones, text="▶ Iniciar Riego", command=self.cmd_iniciar_riego)
        btn_iniciar.grid(row=1, column=1, padx=5, pady=5)

        btn_bloquear = ttk.Button(frame_acciones, text="⛔ Bloquear (Fuera Servicio)", command=self.cmd_bloquear_ws)
        btn_bloquear.grid(row=1, column=2, padx=5, pady=5)

        btn_activar = ttk.Button(frame_acciones, text="✅ Activar WS", command=self.cmd_activar_ws)
        btn_activar.grid(row=1, column=3, padx=5, pady=5)

        # 4. Consola de Registro
        frame_console = ttk.LabelFrame(self.root, text=" Registro de Eventos (Sockets & Kafka) ", padding=5)
        frame_console.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.txt_console = tk.Text(
            frame_console, 
            height=8, 
            bg="#1e1e1e", 
            fg="#dcdcdc", 
            insertbackground="white",
            font=("Consolas", 9)
        )
        scrollbar_console = ttk.Scrollbar(frame_console, orient=tk.VERTICAL, command=self.txt_console.yview)
        self.txt_console.configure(yscroll=scrollbar_console.set)

        self.txt_console.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar_console.pack(side=tk.RIGHT, fill=tk.Y)

        # 5. Barra de Estado
        self.lbl_resumen = tk.Label(
            self.root, 
            text="Total: 0 | Disponibles: 0 | Regando: 0 | Incidencias: 0 | Desconectadas: 0", 
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

    def cargar_estaciones_bd(self):
        """Lee de la BD las estaciones registradas y las carga inicialmente como DESCONECTADA."""
        for item in self.tree.get_children():
            self.tree.delete(item)

        estaciones = obtener_estaciones()
        if estaciones:
            for est in estaciones:
                est_id, ubicacion, _ = est[0], est[1], est[2]
                self.tree.insert("", tk.END, iid=est_id, values=(est_id, ubicacion, "DESCONECTADA", "0.0", "0.0", "-"), tags=("DESCONECTADA",))
        self.actualizar_contadores()

    def registrar_estacion_manual(self):
        ws_id = self.entry_id.get().strip()
        ubicacion = self.entry_ubicacion.get().strip()
        if ws_id and ubicacion:
            guardar_o_actualizar_estacion(ws_id, ubicacion=ubicacion, estado="DESCONECTADA")
            self.actualizar_estacion(ws_id, "DESCONECTADA", ubicacion=ubicacion)
            self.log(f"[CENTRAL] Estación {ws_id} dada de alta en BD.")
            self.entry_id.delete(0, tk.END)
            self.entry_ubicacion.delete(0, tk.END)

    def actualizar_estacion(self, ws_id, estado, ubicacion=None, caudal="0.0", volumen="0.0", operario="-"):
        def _update():
            if self.tree.exists(ws_id):
                val = self.tree.item(ws_id, "values")
                ubi = ubicacion if ubicacion else val[1]
                cau = caudal if caudal != "0.0" else val[3] if estado == "REGANDO" else "0.0"
                vol = volumen if volumen != "0.0" else val[4] if estado == "REGANDO" else "0.0"
                ope = operario if operario != "-" else val[5] if estado == "REGANDO" else "-"
                
                tag_color = "REGANDO_A" if estado == "REGANDO" else estado
                self.tree.item(ws_id, values=(ws_id, ubi, estado, cau, vol, ope), tags=(tag_color,))
            else:
                ubi = ubicacion if ubicacion else "Parque Central"
                tag_color = "REGANDO_A" if estado == "REGANDO" else estado
                self.tree.insert("", tk.END, iid=ws_id, values=(ws_id, ubi, estado, caudal, volumen, operario), tags=(tag_color,))
            
            self.actualizar_contadores()

        self.root.after(0, _update)

    def _iniciar_bucle_parpadeo(self):
        """Alterna tonos de verde cada 500ms para simular el parpadeo al regar."""
        def _parpadear():
            self.fase_parpadeo = not self.fase_parpadeo
            tag_fase = "REGANDO_A" if self.fase_parpadeo else "REGANDO_B"

            for item in self.tree.get_children():
                val = self.tree.item(item, "values")
                if val and val[2] == "REGANDO":
                    self.tree.item(item, tags=(tag_fase,))

            self.root.after(500, _parpadear)

        self.root.after(500, _parpadear)

    def actualizar_contadores(self):
        items = self.tree.get_children()
        total = len(items)
        ok, regando, ko, desconectadas = 0, 0, 0, 0

        for item in items:
            val = self.tree.item(item, "values")
            if val:
                est = val[2]
                if est in ["OK", "CONECTADA"]:
                    ok += 1
                elif est == "REGANDO":
                    regando += 1
                elif est in ["KO", "FUGA", "AVERIA", "FUERA_SERVICIO"]:
                    ko += 1
                elif est == "DESCONECTADA":
                    desconectadas += 1

        self.lbl_resumen.config(
            text=f"Total: {total}  |  Disponibles: {ok}  |  Regando: {regando}  |  Incidencias: {ko}  |  Desconectadas: {desconectadas}"
        )

    def cmd_iniciar_riego(self):
        ws_id = self.entry_id.get().strip()
        if ws_id:
            self.central_app.solicitar_riego_central(ws_id, duracion=10)

    def cmd_bloquear_ws(self):
        ws_id = self.entry_id.get().strip()
        if ws_id:
            self.central_app.bloquear_estacion(ws_id)

    def cmd_activar_ws(self):
        ws_id = self.entry_id.get().strip()
        if ws_id:
            self.central_app.activar_estacion(ws_id)


class WM_Central:
    def __init__(self, puerto_sockets: int, bootstrap_kafka: str):
        self.puerto_sockets = puerto_sockets
        self.bootstrap_kafka = bootstrap_kafka
        self.producer = None
        self.dashboard = None
        self.ejecutando = True

    def log(self, texto):
        print(texto)
        if self.dashboard:
            self.dashboard.log(texto)

    def iniciar_consumidor_kafka(self):
        self.log(f"[KAFKA CENTRAL] Conectando a {self.bootstrap_kafka}...")
        consumer = None
        while consumer is None and self.ejecutando:
            try:
                consumer = KafkaConsumer(
                    'wm-orders-request',
                    'wm-telemetry',
                    'wm-alerts-events',
                    bootstrap_servers=self.bootstrap_kafka,
                    value_deserializer=deserializar_json,
                    group_id='central-group',
                    auto_offset_reset='latest'
                )
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_kafka,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                self.log("[KAFKA CENTRAL] Conectado exitosamente.")
            except Exception:
                time.sleep(3)

        for msg in consumer:
            if not self.ejecutando:
                break
            topico = msg.topic
            datos = msg.value
            if not datos:
                continue

            if topico == 'wm-orders-request':
                order_id = datos.get("order_id")
                ws_id = datos.get("ws_id")
                operator_id = datos.get("operator_id")
                duracion = datos.get("duration_seconds", 10)

                self.log(f"[KAFKA] Solicitud de riego de {operator_id} para {ws_id}")

                estaciones = obtener_estaciones()
                estado_actual = "DESCONECTADA"
                for est in estaciones:
                    if est[0] == ws_id:
                        estado_actual = est[2]

                if estado_actual in ["OK", "CONECTADA"]:
                    autorizado = True
                    razon = "Estación disponible"
                    guardar_o_actualizar_estacion(ws_id, estado="REGANDO")
                    if self.dashboard:
                        self.dashboard.actualizar_estacion(ws_id, "REGANDO", operario=operator_id)
                else:
                    autorizado = False
                    razon = f"Estación no disponible (Estado: {estado_actual})"

                respuesta = {
                    "order_id": order_id,
                    "ws_id": ws_id,
                    "operator_id": operator_id,
                    "status": "AUTHORIZED" if autorizado else "DENIED",
                    "duration_seconds": duracion,
                    "reason": razon
                }
                self.producer.send('wm-orders-response', key=ws_id.encode('utf-8'), value=respuesta)

            elif topico == 'wm-telemetry':
                ws_id = datos.get("ws_id")
                caudal = str(datos.get("flow_rate_lmin", "0.0"))
                volumen = str(datos.get("accumulated_volume_l", "0.0"))

                if self.dashboard and self.dashboard.tree.exists(ws_id):
                    val = self.dashboard.tree.item(ws_id, "values")
                    if val and val[2] != "DESCONECTADA":
                        self.dashboard.actualizar_estacion(ws_id, "REGANDO", caudal=caudal, volumen=volumen)

            elif topico == 'wm-alerts-events':
                if datos.get("event") == "WATERING_FINISHED":
                    ws_id = datos.get("ws_id")
                    operador = datos.get("operator_id")
                    motivo = datos.get("status")
                    duracion = datos.get("duration_seconds", 0)
                    volumen = datos.get("total_volume_l", 0.0)

                    self.log(f"[KAFKA] Fin de riego en {ws_id} ({operador}) -> {motivo}")

                    estaciones = obtener_estaciones()
                    estado_actual = "DESCONECTADA"
                    for est in estaciones:
                        if est[0] == ws_id:
                            estado_actual = est[2]

                    if estado_actual != "DESCONECTADA":
                        nuevo_est = "FUGA" if motivo == "ABORTED_LEAK" else "OK"
                        guardar_o_actualizar_estacion(ws_id, estado=nuevo_est)
                        if self.dashboard:
                            self.dashboard.actualizar_estacion(ws_id, nuevo_est, caudal="0.0", volumen="0.0", operario="-")

                    resumen_operario = {
                        "event": "WATERING_SUMMARY",
                        "ws_id": ws_id,
                        "operator_id": operador,
                        "status": motivo,
                        "duration_seconds": duracion,
                        "total_volume_l": volumen
                    }
                    self.producer.send('wm-orders-response', key=ws_id.encode('utf-8'), value=resumen_operario)

    def atender_monitor(self, conn, addr):
        self.log(f"[SOCKETS CENTRAL] Conexión desde {addr}")
        ws_id = None
        try:
            while self.ejecutando:
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

                if mensaje.startswith("AUTH#"):
                    partes = mensaje.split("#")
                    ws_id = partes[1]
                    estado_inicial = partes[2] if len(partes) > 2 else "AVERIA"

                    guardar_o_actualizar_estacion(ws_id, estado=estado_inicial)
                    if self.dashboard:
                        self.dashboard.actualizar_estacion(ws_id, estado_inicial)

                    conn.send(empaquetar("OK#AUTENTICADO"))
                    conn.recv(1024)

                elif mensaje.startswith("STATUS#"):
                    partes = mensaje.split("#")
                    ws_id = partes[1]
                    nuevo_estado = partes[2]

                    guardar_o_actualizar_estacion(ws_id, estado=nuevo_estado)
                    if self.dashboard:
                        self.dashboard.actualizar_estacion(ws_id, nuevo_estado)

                    conn.send(empaquetar("OK#ESTADO_RECIBIDO"))
                    conn.recv(1024)

                elif mensaje.startswith("LEAK#"):
                    partes = mensaje.split("#")
                    ws_id = partes[1]

                    guardar_o_actualizar_estacion(ws_id, estado="FUGA")
                    if self.dashboard:
                        self.dashboard.actualizar_estacion(ws_id, "FUGA")

                    self.log(f"[ALERTA SOCKETS] ¡Fuga detectada en {ws_id}!")
                    conn.send(empaquetar("OK#FUGA_REGISTRADA"))
                    conn.recv(1024)

        except Exception as e:
            self.log(f"[ERROR SOCKET CENTRAL] {addr}: {e}")
        finally:
            if ws_id:
                guardar_o_actualizar_estacion(ws_id, estado="DESCONECTADA")
                if self.dashboard:
                    self.dashboard.actualizar_estacion(ws_id, "DESCONECTADA", caudal="0.0", volumen="0.0", operario="-")
            conn.close()

    def iniciar_servidor_sockets(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', self.puerto_sockets))
        server.listen()
        server.settimeout(1.0)

        self.log(f"[SOCKETS CENTRAL] Escuchando monitores en puerto {self.puerto_sockets}...")

        while self.ejecutando:
            try:
                conn, addr = server.accept()
                conn.settimeout(None)
                threading.Thread(target=self.atender_monitor, args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue

        server.close()

    # --- COMANDOS ARBITRARIOS (PUNTO 11 DEL PDF) ---
    def solicitar_riego_central(self, ws_id, duracion=10):
        if self.producer:
            orden = {
                "order_id": f"ORD-CENTRAL-{int(time.time())}",
                "operator_id": "CENTRAL_ADMIN",
                "ws_id": ws_id,
                "duration_seconds": duracion
            }
            self.producer.send('wm-orders-request', key=ws_id.encode('utf-8'), value=orden)
            self.log(f"[CENTRAL PUNTO 11] Riego arbitrario ordenado para {ws_id}")

    def bloquear_estacion(self, ws_id):
        guardar_o_actualizar_estacion(ws_id, estado="FUERA_SERVICIO")
        if self.dashboard:
            self.dashboard.actualizar_estacion(ws_id, "FUERA_SERVICIO")
        self.log(f"[CENTRAL PUNTO 11] Estación {ws_id} BLOQUEADA (Fuera de Servicio).")

    def activar_estacion(self, ws_id):
        guardar_o_actualizar_estacion(ws_id, estado="OK")
        if self.dashboard:
            self.dashboard.actualizar_estacion(ws_id, "OK")
        self.log(f"[CENTRAL PUNTO 11] Estación {ws_id} ACTIVADA (Disponible).")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python WM_Central.py <puerto_sockets> [<ip_kafka> <puerto_kafka>]")
        sys.exit(1)

    puerto_sockets = int(sys.argv[1])
    ip_kafka = sys.argv[2] if len(sys.argv) > 2 else "localhost"
    puerto_kafka = sys.argv[3] if len(sys.argv) > 3 else "9092"
    bootstrap_kafka = f"{ip_kafka}:{puerto_kafka}"

    init_db()

    app_central = WM_Central(puerto_sockets, bootstrap_kafka)

    threading.Thread(target=app_central.iniciar_servidor_sockets, daemon=True).start()
    threading.Thread(target=app_central.iniciar_consumidor_kafka, daemon=True).start()

    root = tk.Tk()
    dashboard = DashboardCentral(root, app_central)
    app_central.dashboard = dashboard

    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("\n[CENTRAL] Apagado limpio.")
        sys.exit(0)