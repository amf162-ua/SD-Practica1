import socket
import sys
import threading
import json
import time
import os
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

DIRECTORIO_ACTUAL = os.path.dirname(os.path.abspath(__file__))
if DIRECTORIO_ACTUAL not in sys.path:
    sys.path.insert(0, DIRECTORIO_ACTUAL)

from kafka import KafkaConsumer, KafkaProducer
from protocolo import empaquetar, desempaquetar, ACK, NACK, ENQ, EOT
from database import init_db, obtener_estaciones, guardar_o_actualizar_estacion

# Intentar cargar Tkinter solo si hay entorno gráfico disponible
GUI_DISPONIBLE = False
try:
    import tkinter as tk
    from tkinter import ttk
    # En Linux/Docker sin display, no hay GUI
    if os.name != 'nt' and not os.environ.get('DISPLAY'):
        GUI_DISPONIBLE = False
    else:
        GUI_DISPONIBLE = True
except (ImportError, Exception):
    GUI_DISPONIBLE = False


def deserializar_json(mensaje_bytes):
    """Convierte los bytes recibidos de Kafka en un diccionario Python."""
    if mensaje_bytes is None:
        return None
    try:
        return json.loads(mensaje_bytes.decode('utf-8'))
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# INTERFAZ GRÁFICA TKINTER — Panel de Control Central (PDF páginas 4-6)
# ─────────────────────────────────────────────────────────────────────────────
class DashboardCentral:
    """
    Panel de monitorización y control del sistema.
    Muestra todas las WS con sus estados en tiempo real usando colores
    según la especificación del enunciado (página 4):
      - VERDE        → Disponible / Activada (OK)
      - VERDE PARPADEO → Regando
      - ROJO         → Fuga detectada (KO/FUGA)
      - NARANJA      → Fuera de Servicio / Avería
      - GRIS         → Desconectada
    """
    def __init__(self, root, central_app):
        self.root = root
        self.central_app = central_app
        self.root.title("WaterManagement — Panel de Control Central")
        self.root.geometry("950x700")
        self.root.configure(bg="#1a1a2e")

        # Colores de estado según especificación PDF (página 4)
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
        # ── Cabecera ──
        tk.Label(
            self.root,
            text="SISTEMA CENTRAL DE MONITORIZACIÓN Y CONTROL — WaterManagement",
            font=("Helvetica", 11, "bold"),
            bg="#2c3e50", fg="white", pady=8
        ).pack(fill=tk.X)

        # ── Tabla de Estaciones (Treeview) ──
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Treeview", rowheight=26, font=("Helvetica", 9))
        style.configure("Treeview.Heading", font=("Helvetica", 9, "bold"))

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

        sb = ttk.Scrollbar(frame_tabla, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=sb.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb.pack(side=tk.RIGHT, fill=tk.Y)

        # Configurar colores por tag para cada estado
        for estado, color in self.colores_estado.items():
            self.tree.tag_configure(estado, background=color, foreground="white")

        # Al hacer clic en una fila, rellenar automáticamente el campo ID
        self.tree.bind("<<TreeviewSelect>>", self._al_seleccionar_estacion)

        # ── Panel de Acciones Arbitrarias — Punto 11 del enunciado ──
        frame_acciones = ttk.LabelFrame(
            self.root, text=" Órdenes de Central (Punto 11) ", padding=5
        )
        frame_acciones.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame_acciones, text="ID WS:").grid(row=0, column=0, padx=5, pady=2)
        self.entry_id = ttk.Entry(frame_acciones, width=12)
        self.entry_id.grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(frame_acciones, text="Ubicación:").grid(row=0, column=2, padx=5, pady=2)
        self.entry_ubicacion = ttk.Entry(frame_acciones, width=20)
        self.entry_ubicacion.grid(row=0, column=3, padx=5, pady=2)

        ttk.Button(
            frame_acciones, text="Alta / Registro BD",
            command=self.registrar_estacion_manual
        ).grid(row=0, column=4, padx=5, pady=2)

        # Los 3 botones de la Punto 11
        ttk.Button(
            frame_acciones, text="▶ Iniciar Riego",
            command=self.cmd_iniciar_riego
        ).grid(row=1, column=1, padx=5, pady=5)

        ttk.Button(
            frame_acciones, text="Bloquear (Fuera de Servicio)",
            command=self.cmd_bloquear_ws
        ).grid(row=1, column=2, padx=5, pady=5)

        ttk.Button(
            frame_acciones, text="Activar WS",
            command=self.cmd_activar_ws
        ).grid(row=1, column=3, padx=5, pady=5)

        # ── Consola de Eventos ──
        frame_console = ttk.LabelFrame(
            self.root, text=" Registro de Eventos (Sockets & Kafka) ", padding=5
        )
        frame_console.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.txt_console = tk.Text(
            frame_console, height=8,
            bg="#1e1e1e", fg="#dcdcdc",
            insertbackground="white",
            font=("Consolas", 9)
        )
        sb2 = ttk.Scrollbar(frame_console, orient=tk.VERTICAL, command=self.txt_console.yview)
        self.txt_console.configure(yscroll=sb2.set)
        self.txt_console.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        sb2.pack(side=tk.RIGHT, fill=tk.Y)

        # ── Barra de resumen inferior ──
        self.lbl_resumen = tk.Label(
            self.root,
            text="Total: 0 | Disponibles: 0 | Regando: 0 | Incidencias: 0 | Desconectadas: 0",
            bd=1, relief=tk.SUNKEN, anchor=tk.W, padx=10, pady=4
        )
        self.lbl_resumen.pack(side=tk.BOTTOM, fill=tk.X)

    def log(self, mensaje):
        """Añade un mensaje a la consola de eventos (thread-safe mediante after)."""
        def _append():
            self.txt_console.insert(tk.END, mensaje + "\n")
            self.txt_console.see(tk.END)
        self.root.after(0, _append)

    def cargar_estaciones_bd(self):
        """Carga las estaciones conocidas de la BD mostrándolas como DESCONECTADA (requisito PDF)."""
        for item in self.tree.get_children():
            self.tree.delete(item)
        for est in obtener_estaciones():
            ws_id, ubicacion, _ = est[0], est[1], est[2]
            self.tree.insert(
                "", tk.END, iid=ws_id,
                values=(ws_id, ubicacion, "DESCONECTADA", "0.0", "0.0", "-"),
                tags=("DESCONECTADA",)
            )
        self.actualizar_contadores()

    def registrar_estacion_manual(self):
        """Da de alta manualmente una estación en la BD desde el panel."""
        ws_id = self.entry_id.get().strip()
        ubicacion = self.entry_ubicacion.get().strip()
        if ws_id and ubicacion:
            guardar_o_actualizar_estacion(ws_id, ubicacion=ubicacion, estado="DESCONECTADA")
            self.actualizar_estacion(ws_id, "DESCONECTADA", ubicacion=ubicacion)
            self.log(f"[CENTRAL] Estación {ws_id} dada de alta en BD.")
            self.entry_id.delete(0, tk.END)
            self.entry_ubicacion.delete(0, tk.END)

    def _al_seleccionar_estacion(self, event):
        """Al seleccionar una fila del Treeview, rellena los campos de ID y ubicación."""
        seleccion = self.tree.selection()
        if seleccion:
            val = self.tree.item(seleccion[0], "values")
            if val:
                self.entry_id.delete(0, tk.END)
                self.entry_id.insert(0, val[0])
                if len(val) > 1:
                    self.entry_ubicacion.delete(0, tk.END)
                    self.entry_ubicacion.insert(0, val[1])

    def actualizar_estacion(self, ws_id, estado, ubicacion=None, caudal="0.0", volumen="0.0", operario="-"):
        """Actualiza o inserta una fila en el Treeview (thread-safe)."""
        def _update():
            try:
                tag_color = "REGANDO_A" if estado == "REGANDO" else estado
                if self.tree.exists(ws_id):
                    val = self.tree.item(ws_id, "values")
                    ubi = ubicacion if ubicacion else (val[1] if len(val) > 1 else "—")
                    # Preservar caudal/volumen/operario si sigue regando y no vienen nuevos
                    if estado == "REGANDO":
                        cau = caudal if caudal != "0.0" else (val[3] if len(val) > 3 else "0.0")
                        vol = volumen if volumen != "0.0" else (val[4] if len(val) > 4 else "0.0")
                        ope = operario if operario != "-" else (val[5] if len(val) > 5 else "-")
                    else:
                        cau, vol, ope = "0.0", "0.0", "-"
                    self.tree.item(ws_id, values=(ws_id, ubi, estado, cau, vol, ope), tags=(tag_color,))
                else:
                    ubi = ubicacion if ubicacion else "—"
                    self.tree.insert(
                        "", tk.END, iid=ws_id,
                        values=(ws_id, ubi, estado, caudal, volumen, operario),
                        tags=(tag_color,)
                    )
                self.actualizar_contadores()
            except Exception as e:
                print(f"[ERROR UI] {e}")
        self.root.after(0, _update)

    def _iniciar_bucle_parpadeo(self):
        """Alterna entre REGANDO_A y REGANDO_B cada 500ms para simular el parpadeo verde."""
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
        """Actualiza la barra de resumen inferior con totales por estado."""
        items = self.tree.get_children()
        total = len(items)
        ok = regando = ko = desconectadas = 0
        for item in items:
            val = self.tree.item(item, "values")
            if val:
                est = val[2]
                if est in ("OK", "CONECTADA"):
                    ok += 1
                elif est == "REGANDO":
                    regando += 1
                elif est in ("KO", "FUGA", "AVERIA", "FUERA_SERVICIO"):
                    ko += 1
                elif est == "DESCONECTADA":
                    desconectadas += 1
        self.lbl_resumen.config(
            text=f"Total: {total}  |  Disponibles: {ok}  |  Regando: {regando}  |  Incidencias: {ko}  |  Desconectadas: {desconectadas}"
        )

    # ── Botones Punto 11 ──
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


# ─────────────────────────────────────────────────────────────────────────────
# NÚCLEO DEL SISTEMA CENTRAL
# ─────────────────────────────────────────────────────────────────────────────
class WM_Central:
    """
    Módulo Central del sistema WaterManagement.
    Implementa:
    - Servidor TCP de sockets para comunicación con los Monitores (WM_WS_M).
    - Consumidor/Productor Kafka para gestión de órdenes de riego, telemetría y alertas.
    - Panel de control gráfico (Tkinter) o modo consola (headless/Cloud).
    - Punto 11: comandos arbitrarios (iniciar riego, bloquear, activar WS).
    """
    def __init__(self, puerto_sockets: int, bootstrap_kafka: str):
        self.puerto_sockets = puerto_sockets
        self.bootstrap_kafka = bootstrap_kafka
        self.producer = None
        self.dashboard = None
        self.ejecutando = True

    def log(self, texto):
        """Imprime en consola y, si hay GUI, también en el panel de eventos."""
        t = time.strftime("[%H:%M:%S]")
        print(f"{t} {texto}", flush=True)
        if self.dashboard:
            self.dashboard.log(texto)

    # ── Kafka ──────────────────────────────────────────────────────────────
    def iniciar_consumidor_kafka(self):
        """
        Suscribe a los tres tópicos de entrada:
          - wm-orders-request  → peticiones de riego de los operarios (FO)
          - wm-telemetry       → telemetría en tiempo real de los Engines (WS_E)
          - wm-alerts-events   → eventos de fin de riego / fuga de los Engines
        """
        self.log(f"[KAFKA] Conectando al broker {self.bootstrap_kafka}...")
        consumer = None
        while consumer is None and self.ejecutando:
            try:
                consumer = KafkaConsumer(
                    'wm-orders-request',
                    'wm-telemetry',
                    'wm-alerts-events',
                    bootstrap_servers=self.bootstrap_kafka,
                    value_deserializer=deserializar_json,
                    group_id='central-group-v1',
                    auto_offset_reset='latest'
                )
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_kafka,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                self.log("[KAFKA] Conexión establecida correctamente.")
            except Exception as e:
                self.log(f"[KAFKA] Reintentando conexión ({e})...")
                time.sleep(3)

        for msg in consumer:
            if not self.ejecutando:
                break
            topico = msg.topic
            datos = msg.value
            if not datos:
                continue

            # ── 1. Petición de riego de un Operario (FO) ──────────────────
            if topico == 'wm-orders-request':
                self._procesar_solicitud_riego(datos)

            # ── 2. Telemetría en tiempo real del Engine ────────────────────
            elif topico == 'wm-telemetry':
                self._procesar_telemetria(datos)

            # ── 3. Evento de fin de riego del Engine ──────────────────────
            elif topico == 'wm-alerts-events':
                self._procesar_alerta(datos)

    def _procesar_solicitud_riego(self, datos):
        """
        Comprueba disponibilidad de la WS y autoriza o deniega el riego.
        Responde publicando en 'wm-orders-response'.
        """
        order_id   = datos.get("order_id")
        ws_id      = datos.get("ws_id")
        operator_id = datos.get("operator_id")
        duracion   = datos.get("duration_seconds", 10)

        self.log(f"[SOLICITUD RIEGO] Operario: {operator_id} → Estación: {ws_id}")

        # Consultar estado actual en BD
        estado_actual = "DESCONECTADA"
        for est in obtener_estaciones():
            if est[0] == ws_id:
                estado_actual = est[2]
                break

        if estado_actual in ("OK", "CONECTADA"):
            autorizado = True
            razon = "Estación disponible"
            guardar_o_actualizar_estacion(ws_id, estado="REGANDO")
            if self.dashboard:
                self.dashboard.actualizar_estacion(ws_id, "REGANDO", operario=operator_id)
            self.log(f"[AUTORIZADO] Riego en {ws_id} para operario {operator_id}.")
        else:
            autorizado = False
            razon = f"Estación no disponible (estado: {estado_actual})"
            self.log(f"[DENEGADO] Riego en {ws_id}. Motivo: {razon}")

        respuesta = {
            "order_id":        order_id,
            "ws_id":           ws_id,
            "operator_id":     operator_id,
            "status":          "AUTHORIZED" if autorizado else "DENIED",
            "duration_seconds": duracion,
            "reason":          razon
        }
        if self.producer:
            self.producer.send('wm-orders-response', key=ws_id.encode('utf-8'), value=respuesta)

    def _procesar_telemetria(self, datos):
        """Actualiza caudal y volumen acumulado en el panel solo si la WS está regando."""
        ws_id  = datos.get("ws_id")
        caudal = str(datos.get("flow_rate_lmin", "0.0"))
        volumen = str(datos.get("accumulated_volume_l", "0.0"))

        # Solo actualizar si el estado en BD sigue siendo REGANDO (evita sobreescribir FUGA/BLOQUEADA)
        estado_bd = "DESCONECTADA"
        for est in obtener_estaciones():
            if est[0] == ws_id:
                estado_bd = est[2]
                break

        if estado_bd == "REGANDO" and self.dashboard:
            self.dashboard.actualizar_estacion(ws_id, "REGANDO", caudal=caudal, volumen=volumen)

    def _procesar_alerta(self, datos):
        """Procesa el fin de riego: actualiza estado, notifica al operario con resumen."""
        if datos.get("event") != "WATERING_FINISHED":
            return

        ws_id      = datos.get("ws_id")
        operador   = datos.get("operator_id")
        motivo     = datos.get("status")
        duracion   = datos.get("duration_seconds", 0)
        volumen    = datos.get("total_volume_l", 0.0)

        self.log(f"[FIN RIEGO] Estación: {ws_id} | Operario: {operador} | Motivo: {motivo}")

        # Determinar nuevo estado: FUGA si fue abortado por fuga, OK si fue normal
        estado_bd = "DESCONECTADA"
        for est in obtener_estaciones():
            if est[0] == ws_id:
                estado_bd = est[2]
                break

        if estado_bd != "DESCONECTADA":
            nuevo_est = "FUGA" if motivo == "ABORTED_LEAK" else "OK"
            guardar_o_actualizar_estacion(ws_id, estado=nuevo_est)
            if self.dashboard:
                self.dashboard.actualizar_estacion(ws_id, nuevo_est, caudal="0.0", volumen="0.0", operario="-")

        # Enviar resumen final al operario
        if self.producer:
            resumen = {
                "event":            "WATERING_SUMMARY",
                "ws_id":            ws_id,
                "operator_id":      operador,
                "status":           motivo,
                "duration_seconds": duracion,
                "total_volume_l":   volumen
            }
            self.producer.send('wm-orders-response', key=ws_id.encode('utf-8'), value=resumen)

    # ── Sockets ────────────────────────────────────────────────────────────
    def atender_monitor(self, conn, addr):
        """
        Gestiona la conexión persistente con un Monitor (WM_WS_M).
        Protocolo: ENQ → ACK → <trama STX/ETX/LRC> → ACK/NACK → <respuesta> → ACK del Monitor.
        Mensajes soportados: AUTH, STATUS, LEAK.
        """
        self.log(f"[SOCKETS] Nueva conexión desde {addr}")
        ws_id = None
        try:
            while self.ejecutando:
                trama = conn.recv(1024)
                if not trama:
                    break  # El Monitor cerró la conexión

                # ── Señal ENQ: solicitud de permiso para transmitir ──
                if trama == ENQ:
                    conn.send(ACK)
                    continue

                # ── Señal EOT: fin de transmisión ──
                if trama == EOT:
                    break

                # ── Trama de datos: desempaquetar y validar LRC ──
                mensaje, valido = desempaquetar(trama)
                if not valido:
                    conn.send(NACK)
                    continue
                conn.send(ACK)  # ACK de recepción correcta de la trama

                # ── Procesar mensaje ──
                if mensaje.startswith("AUTH#"):
                    partes = mensaje.split("#")
                    ws_id      = partes[1]
                    ubicacion  = partes[2] if len(partes) > 2 else "—"
                    estado_ini = partes[3] if len(partes) > 3 else "AVERIA"

                    guardar_o_actualizar_estacion(ws_id, ubicacion=ubicacion, estado=estado_ini)
                    if self.dashboard:
                        self.dashboard.actualizar_estacion(ws_id, estado_ini, ubicacion=ubicacion)
                    self.log(f"[AUTH] Estación {ws_id} autenticada. Ubicación: {ubicacion}. Estado: {estado_ini}")

                    # Responder con confirmación y esperar ACK del Monitor
                    conn.send(empaquetar("OK#AUTENTICADO"))
                    ack = conn.recv(1024)
                    if ack != ACK:
                        self.log(f"[WARN] {ws_id}: ACK esperado tras AUTH, recibido: {ack}")

                elif mensaje.startswith("STATUS#"):
                    partes = mensaje.split("#")
                    ws_id       = partes[1]
                    nuevo_estado = partes[2]

                    guardar_o_actualizar_estacion(ws_id, estado=nuevo_estado)
                    if self.dashboard:
                        self.dashboard.actualizar_estacion(ws_id, nuevo_estado)

                    conn.send(empaquetar("OK#ESTADO_RECIBIDO"))
                    ack = conn.recv(1024)
                    if ack != ACK:
                        self.log(f"[WARN] {ws_id}: ACK esperado tras STATUS, recibido: {ack}")

                elif mensaje.startswith("LEAK#"):
                    partes = mensaje.split("#")
                    ws_id = partes[1]

                    guardar_o_actualizar_estacion(ws_id, estado="FUGA")
                    if self.dashboard:
                        self.dashboard.actualizar_estacion(ws_id, "FUGA")
                    self.log(f"[⚠ FUGA DETECTADA] Estación {ws_id} reporta fuga. Riego detenido.")

                    conn.send(empaquetar("OK#FUGA_REGISTRADA"))
                    ack = conn.recv(1024)
                    if ack != ACK:
                        self.log(f"[WARN] {ws_id}: ACK esperado tras LEAK, recibido: {ack}")

        except Exception as e:
            self.log(f"[ERROR SOCKET] {addr}: {e}")
        finally:
            # Al perder la conexión con el Monitor → estación pasa a DESCONECTADA (PDF, punto 1)
            if ws_id:
                guardar_o_actualizar_estacion(ws_id, estado="DESCONECTADA")
                if self.dashboard:
                    self.dashboard.actualizar_estacion(
                        ws_id, "DESCONECTADA", caudal="0.0", volumen="0.0", operario="-"
                    )
                self.log(f"[SOCKETS] Estación {ws_id} desconectada.")
            conn.close()

    def iniciar_servidor_sockets(self):
        """Arranca el servidor TCP que escucha conexiones entrantes de los Monitores."""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind(('0.0.0.0', self.puerto_sockets))
            server.listen()
            server.settimeout(1.0)
            self.log(f"[SOCKETS] Servidor TCP escuchando en puerto {self.puerto_sockets}...")
        except Exception as e:
            self.log(f"[ERROR CRÍTICO] No se pudo vincular al puerto {self.puerto_sockets}: {e}")
            return

        while self.ejecutando:
            try:
                conn, addr = server.accept()
                conn.settimeout(None)
                threading.Thread(target=self.atender_monitor, args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue

        server.close()

    # ── Comandos Arbitrarios — Punto 11 del enunciado ─────────────────────
    def solicitar_riego_central(self, ws_id: str, duracion: int = 10):
        """
        Inicia un riego desde la propia Central (Punto 11).
        Publica la orden en Kafka como si fuera un operario con ID 'CENTRAL_ADMIN'.
        """
        if self.producer:
            orden = {
                "order_id":        f"ORD-CENTRAL-{int(time.time())}",
                "operator_id":     "CENTRAL_ADMIN",
                "ws_id":           ws_id,
                "duration_seconds": duracion
            }
            self.producer.send('wm-orders-request', key=ws_id.encode('utf-8'), value=orden)
            self.log(f"[PUNTO 11] Orden de riego enviada para {ws_id} ({duracion}s).")
        else:
            self.log("[ERROR] Productor Kafka no inicializado aún.")

    def bloquear_estacion(self, ws_id: str):
        """
        Pone la estación en FUERA_SERVICIO (Punto 11 — color NARANJA).
        El Monitor lo detectará en el siguiente STATUS y propagará el cambio.
        """
        guardar_o_actualizar_estacion(ws_id, estado="FUERA_SERVICIO")
        if self.dashboard:
            self.dashboard.actualizar_estacion(ws_id, "FUERA_SERVICIO")
        self.log(f"[PUNTO 11] Estación {ws_id} → FUERA DE SERVICIO (bloqueada).")

    def activar_estacion(self, ws_id: str):
        """Vuelve a poner la estación en estado OK (disponible) (Punto 11 — color VERDE)."""
        guardar_o_actualizar_estacion(ws_id, estado="OK")
        if self.dashboard:
            self.dashboard.actualizar_estacion(ws_id, "OK")
        self.log(f"[PUNTO 11] Estación {ws_id} → ACTIVADA (disponible).")

    # ── Modo Consola (Cloud / Headless sin GUI) ───────────────────────────
    def bucle_consola(self):
        """
        Interfaz de texto para controlar Central cuando no hay entorno gráfico
        (Railway, AWS EC2, Docker sin X11).
        """
        print(f"\n[CENTRAL] Modo consola activo — Puerto sockets: {self.puerto_sockets}")
        print(f"[CENTRAL] Broker Kafka: {self.bootstrap_kafka}")
        print("[CENTRAL] Comandos: status | riego <ws_id> [<seg>] | bloquear <ws_id> | activar <ws_id> | alta <ws_id> <ubicacion> | ayuda | salir\n")

        while self.ejecutando:
            try:
                linea = input("central> ").strip()
                if not linea:
                    continue
                partes = linea.split()
                cmd = partes[0].lower()

                if cmd == 'status':
                    estaciones = obtener_estaciones()
                    print("\n─── ESTACIONES EN BD ───────────────────────────────────")
                    for e in estaciones:
                        print(f"  {e[0]:<10} | {e[1]:<25} | {e[2]}")
                    print("────────────────────────────────────────────────────────\n")

                elif cmd == 'riego' and len(partes) >= 2:
                    ws_id = partes[1]
                    duracion = int(partes[2]) if len(partes) > 2 else 10
                    self.solicitar_riego_central(ws_id, duracion)

                elif cmd == 'bloquear' and len(partes) >= 2:
                    self.bloquear_estacion(partes[1])

                elif cmd == 'activar' and len(partes) >= 2:
                    self.activar_estacion(partes[1])

                elif cmd == 'alta' and len(partes) >= 3:
                    ws_id = partes[1]
                    ubi = " ".join(partes[2:])
                    guardar_o_actualizar_estacion(ws_id, ubicacion=ubi, estado="DESCONECTADA")
                    print(f"[CENTRAL] Estación {ws_id} ({ubi}) registrada en BD.")

                elif cmd in ('ayuda', 'help'):
                    print("  status                     → Ver estaciones y sus estados en BD")
                    print("  riego <ws_id> [<seg>]      → Iniciar riego arbitrario (Punto 11)")
                    print("  bloquear <ws_id>           → Poner en FUERA_SERVICIO (Punto 11)")
                    print("  activar <ws_id>            → Volver a estado OK (Punto 11)")
                    print("  alta <ws_id> <ubicacion>   → Registrar estación en BD")
                    print("  salir                      → Detener servidor Central")

                elif cmd in ('salir', 'exit'):
                    self.ejecutando = False
                    break

            except (EOFError, KeyboardInterrupt):
                time.sleep(1)
            except Exception as ex:
                print(f"[ERROR] {ex}")


# ─────────────────────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA
# Uso: python WM_Central.py <puerto_sockets> <ip_kafka> [<puerto_kafka>]
# Variables de entorno soportadas para Railway/AWS:
#   PORT                   → Puerto TCP del servidor de sockets
#   KAFKA_BOOTSTRAP_SERVERS → Bootstrap server completo (host:puerto)
#   KAFKA_HOST + KAFKA_PORT → Alternativa a KAFKA_BOOTSTRAP_SERVERS
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Puerto de sockets: argumento 1, variable PORT, o 5000 por defecto
    puerto_sockets = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 5000))

    # Kafka bootstrap: KAFKA_BOOTSTRAP_SERVERS, o ip_kafka:puerto_kafka
    bootstrap_kafka = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_kafka:
        ip_kafka    = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("KAFKA_HOST", "localhost")
        puerto_kafka = sys.argv[3] if len(sys.argv) > 3 else os.environ.get("KAFKA_PORT", "9092")
        bootstrap_kafka = ip_kafka if ":" in ip_kafka else f"{ip_kafka}:{puerto_kafka}"

    # Inicializar BD SQLite (crea tabla y resetea todos los estados a DESCONECTADA)
    init_db()

    central = WM_Central(puerto_sockets, bootstrap_kafka)

    # Lanzar servidor de sockets y consumidor Kafka en hilos daemon
    threading.Thread(target=central.iniciar_servidor_sockets, daemon=True).start()
    threading.Thread(target=central.iniciar_consumidor_kafka,  daemon=True).start()

    # Con GUI (Windows/Mac local): lanzar Tkinter en el hilo principal
    if GUI_DISPONIBLE:
        try:
            root = tk.Tk()
            dashboard = DashboardCentral(root, central)
            central.dashboard = dashboard
            root.mainloop()
        except Exception as e:
            print(f"[CENTRAL] GUI no disponible ({e}). Usando modo consola.")
            central.bucle_consola()
    else:
        # Sin GUI (Cloud / Railway / AWS EC2): modo consola interactiva
        central.bucle_consola()