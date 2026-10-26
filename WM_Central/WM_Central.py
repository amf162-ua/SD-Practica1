import socket
import sys
import threading
import json
import time
import os
from kafka import KafkaConsumer, KafkaProducer
from protocolo import empaquetar, desempaquetar, ACK, NACK, ENQ, EOT
from database import init_db, obtener_estaciones, guardar_o_actualizar_estacion

# Intentar cargar Tkinter sólo si hay entorno gráfico
GUI_DISPONIBLE = False
try:
    import tkinter as tk
    from tkinter import ttk
    # Si estamos en headless/Linux sin pantalla, Tkinter fallará al inicializar
    if os.name != 'nt' and not os.environ.get('DISPLAY'):
        GUI_DISPONIBLE = False
    else:
        GUI_DISPONIBLE = True
except ImportError:
    GUI_DISPONIBLE = False


def deserializar_json(mensaje_bytes):
    if mensaje_bytes is None:
        return None
    try:
        return json.loads(mensaje_bytes.decode('utf-8'))
    except Exception:
        return None


class WM_Central:
    def __init__(self, puerto_sockets: int, bootstrap_kafka: str):
        self.puerto_sockets = puerto_sockets
        self.bootstrap_kafka = bootstrap_kafka
        self.producer = None
        self.dashboard = None
        self.ejecutando = True

    def log(self, texto):
        t_stamp = time.strftime("[%Y-%m-%d %H:%M:%S]")
        print(f"{t_stamp} {texto}")
        if self.dashboard:
            self.dashboard.log(texto)

    def iniciar_consumidor_kafka(self):
        self.log(f"[KAFKA CENTRAL] Conectando a broker {self.bootstrap_kafka}...")
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
                self.log("[KAFKA CENTRAL] Conexión establecida con éxito.")
            except Exception as e:
                self.log(f"[KAFKA CENTRAL] Reintentando conexión con Kafka ({e})...")
                time.sleep(3)

        for msg in consumer:
            if not self.ejecutando:
                break
            topico = msg.topic
            datos = msg.value
            if not datos:
                continue

            # 1. Petición de riego
            if topico == 'wm-orders-request':
                order_id = datos.get("order_id")
                ws_id = datos.get("ws_id")
                operator_id = datos.get("operator_id")
                duracion = datos.get("duration_seconds", 10)

                self.log(f"[SOLICITUD RIEGO] Operario: {operator_id} -> Estación: {ws_id}")

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
                    razon = f"Estación no disponible (Estado actual: {estado_actual})"

                respuesta = {
                    "order_id": order_id,
                    "ws_id": ws_id,
                    "operator_id": operator_id,
                    "status": "AUTHORIZED" if autorizado else "DENIED",
                    "duration_seconds": duracion,
                    "reason": razon
                }
                self.producer.send('wm-orders-response', key=ws_id.encode('utf-8'), value=respuesta)

            # 2. Telemetría
            elif topico == 'wm-telemetry':
                ws_id = datos.get("ws_id")
                caudal = str(datos.get("flow_rate_lmin", "0.0"))
                volumen = str(datos.get("accumulated_volume_l", "0.0"))

                if self.dashboard and self.dashboard.tree.exists(ws_id):
                    val = self.dashboard.tree.item(ws_id, "values")
                    if val and val[2] != "DESCONECTADA":
                        self.dashboard.actualizar_estacion(ws_id, "REGANDO", caudal=caudal, volumen=volumen)

            # 3. Fin de Riego
            elif topico == 'wm-alerts-events':
                if datos.get("event") == "WATERING_FINISHED":
                    ws_id = datos.get("ws_id")
                    operador = datos.get("operator_id")
                    motivo = datos.get("status")
                    duracion = datos.get("duration_seconds", 0)
                    volumen = datos.get("total_volume_l", 0.0)

                    self.log(f"[FIN RIEGO] Estación: {ws_id} | Operario: {operador} | Motivo: {motivo}")

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
        self.log(f"[SOCKETS] Conexión entrante desde {addr}")
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

                    self.log(f"[ALERTA LEAK] ¡Fuga reportada en la estación {ws_id}!")
                    conn.send(empaquetar("OK#FUGA_REGISTRADA"))
                    conn.recv(1024)

        except Exception as e:
            self.log(f"[ERROR SOCKET] {addr}: {e}")
        finally:
            if ws_id:
                # Regla de la Guía: Si cae el Monitor, la estación pasa a DESCONECTADA
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

        self.log(f"[SOCKETS] Servidor escuchando en puerto {self.puerto_sockets}...")

        while self.ejecutando:
            try:
                conn, addr = server.accept()
                conn.settimeout(None)
                threading.Thread(target=self.atender_monitor, args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue

        server.close()


if __name__ == "__main__":
    # Soporte para variable de entorno PORT en Railway
    puerto_sockets = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 5000))
    ip_kafka = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("KAFKA_HOST", "localhost")
    puerto_kafka = sys.argv[3] if len(sys.argv) > 3 else os.environ.get("KAFKA_PORT", "9092")
    bootstrap_kafka = f"{ip_kafka}:{puerto_kafka}"

    init_db()

    app_central = WM_Central(puerto_sockets, bootstrap_kafka)

    threading.Thread(target=app_central.iniciar_servidor_sockets, daemon=True).start()
    threading.Thread(target=app_central.iniciar_consumidor_kafka, daemon=True).start()

    # Si hay interfaz gráfica disponible (local), lanzamos Tkinter
    if GUI_DISPONIBLE:
        try:
            from WM_Central.WM_Central import DashboardCentral
            root = tk.Tk()
            dashboard = DashboardCentral(root, app_central)
            app_central.dashboard = dashboard
            root.mainloop()
        except Exception as e:
            print(f"[CENTRAL] Modo Servidor/Consola activado ({e}).")
            while True:
                time.sleep(1)
    else:
        print("[CENTRAL] Modo Servidor Cloud (Railway) activado. Escuchando en consola...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[CENTRAL] Apagado limpio.")