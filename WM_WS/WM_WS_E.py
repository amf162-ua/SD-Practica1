import socket
import sys
import time
import threading
import json
import os
from kafka import KafkaConsumer, KafkaProducer

DIRECTORIO_ACTUAL = os.path.dirname(os.path.abspath(__file__))
if DIRECTORIO_ACTUAL not in sys.path:
    sys.path.insert(0, DIRECTORIO_ACTUAL)

from protocolo import empaquetar, desempaquetar, ACK, NACK, ENQ, EOT

def deserializar_json(mensaje_bytes):
    """Convierte los bytes recibidos de Kafka en un diccionario JSON."""
    if mensaje_bytes is None:
        return None
    try:
        return json.loads(mensaje_bytes.decode('utf-8'))
    except Exception:
        return None


class WateringStationEngine:
    def __init__(self, ip_kafka: str, puerto_kafka: str, ip_monitor: str, puerto_monitor: int, ws_id_param: str = None):
        if ":" in str(ip_kafka):
            self.bootstrap_kafka = str(ip_kafka)
        else:
            self.bootstrap_kafka = f"{ip_kafka}:{puerto_kafka}"
        self.ip_monitor = ip_monitor
        self.puerto_monitor = puerto_monitor
        
        # Identificador de la estación
        self.ws_id = ws_id_param
        
        # Estados internos protegidos por Lock
        self.lock = threading.Lock()
        self.estado_salud = "OK"  # 'OK' o 'KO' (Fuga)
        self.regando = False
        
        # Control de hilos
        self.ejecutando = True
        self.hilo_kafka_iniciado = False

    def responder_ping_monitor(self, sock):
        """Atiende los mensajes del Monitor a través del Socket interno."""
        try:
            while self.ejecutando:
                trama = sock.recv(1024)
                if not trama:
                    break

                if trama == ENQ:
                    sock.send(ACK)
                    continue
                elif trama == EOT:
                    break

                mensaje, valido = desempaquetar(trama)
                if not valido:
                    sock.send(NACK)
                    continue
                else:
                    sock.send(ACK)

                # 1. Configuración de ID asignada por el Monitor
                if mensaje.startswith("CONFIG#"):
                    partes = mensaje.split("#")
                    self.ws_id = partes[1]
                    print(f"[ENGINE] ID asignado por el Monitor: {self.ws_id}")

                    # Iniciar consumidor de Kafka una vez conocido el ID
                    if not self.hilo_kafka_iniciado:
                        hilo_kafka = threading.Thread(
                            target=self.escuchar_ordenes_kafka,
                            daemon=True
                        )
                        hilo_kafka.start()
                        self.hilo_kafka_iniciado = True

                    respuesta = empaquetar("OK#ID_RECIBIDO")
                    sock.send(respuesta)
                    sock.recv(1024)

                # 2. Responder al PING periódico de salud
                elif mensaje == "PING":
                    with self.lock:
                        estado_actual = self.estado_salud

                    respuesta = empaquetar(estado_actual)
                    sock.send(respuesta)

                    ack_recibido = sock.recv(1024)
                    if ack_recibido != ACK:
                        pass

        except Exception as e:
            print(f"[ENGINE {self.ws_id}] Conexión cerrada con Monitor: {e}")
        finally:
            # Requisito de seguridad: Si perdemos conexión con el Monitor, cortamos el riego
            with self.lock:
                if self.regando:
                    self.regando = False
                    print(f"\n[CORTE DE EMERGENCIA {self.ws_id}] Conexión perdida con el Monitor. Riego abortado.")

    def publicar_telemetria_kafka(self, duracion_segundos=10, caudal_lmin=15.0, operator_id="DESCONOCIDO"):
        """Emite lecturas de caudal y volumen acumulado cada 1s y genera el evento final."""
        with self.lock:
            self.regando = True

        print(f"\n[ENGINE {self.ws_id}] ELECTROVÁLVULA ABIERTA — Iniciando riego ({duracion_segundos}s solicitados por {operator_id})...")

        producer = None
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_kafka,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except Exception as e:
            print(f"[ENGINE {self.ws_id}] Error conectando productor Kafka: {e}")
            with self.lock:
                self.regando = False
            return

        volumen_acumulado = 0.0
        segundos_transcurridos = 0

        while segundos_transcurridos < duracion_segundos:
            with self.lock:
                if not self.regando or self.estado_salud != "OK":
                    break

            time.sleep(1)
            segundos_transcurridos += 1
            volumen_acumulado += (caudal_lmin / 60.0)  # Convertir L/min a L/segundo

            payload = {
                "ws_id": self.ws_id,
                "flow_rate_lmin": caudal_lmin,
                "accumulated_volume_l": round(volumen_acumulado, 2),
                "elapsed_seconds": segundos_transcurridos
            }

            try:
                producer.send('wm-telemetry', key=self.ws_id.encode('utf-8'), value=payload)
                print(f"[ENGINE {self.ws_id}] Telemetría -> Caudal: {caudal_lmin} L/min | Acumulado: {round(volumen_acumulado, 2)} L")
            except Exception as e:
                print(f"[ENGINE {self.ws_id}] Error al enviar telemetría: {e}")

        with self.lock:
            self.regando = False
            estado_final_salud = self.estado_salud

        print(f"[ENGINE {self.ws_id}] ELECTROVÁLVULA CERRADA — Riego finalizado.\n")

        # Evaluar motivo de finalización
        if estado_final_salud != "OK":
            motivo_fin = "ABORTED_LEAK"
        elif segundos_transcurridos >= duracion_segundos:
            motivo_fin = "COMPLETED"
        else:
            motivo_fin = "STOPPED_MANUAL"

        # Notificar fin de riego a Kafka
        resumen = {
            "event": "WATERING_FINISHED",
            "ws_id": self.ws_id,
            "operator_id": operator_id,
            "status": motivo_fin,
            "duration_seconds": segundos_transcurridos,
            "total_volume_l": round(volumen_acumulado, 2)
        }

        try:
            producer.send('wm-alerts-events', key=self.ws_id.encode('utf-8'), value=resumen)
            producer.flush()
            print(f"[ENGINE {self.ws_id}] Evento de cierre enviado a 'wm-alerts-events': {motivo_fin}\n")
        except Exception as e:
            print(f"[ENGINE {self.ws_id}] Error al enviar resumen final: {e}\n")

    def escuchar_ordenes_kafka(self):
        """Escucha en Kafka las respuestas de autorización expedidas por Central."""
        consumer = None
        while consumer is None and self.ejecutando:
            try:
                consumer = KafkaConsumer(
                    'wm-orders-response',
                    bootstrap_servers=self.bootstrap_kafka,
                    value_deserializer=deserializar_json,
                    group_id=f'engine-group-{self.ws_id}',
                    auto_offset_reset='latest'
                )
                print(f"[ENGINE {self.ws_id}] Escuchando órdenes en 'wm-orders-response'...")
            except Exception:
                time.sleep(3)

        for msg in consumer:
            if not self.ejecutando:
                break
            datos = msg.value
            if not datos:
                continue

            # Iniciar riego únicamente si la orden es para esta WS y está autorizada
            if datos.get("ws_id") == self.ws_id and datos.get("status") == "AUTHORIZED":
                duracion = datos.get("duration_seconds", 10)
                operator_id = datos.get("operator_id", "DESCONOCIDO")

                hilo_riego = threading.Thread(
                    target=self.publicar_telemetria_kafka,
                    args=(duracion, 15.0, operator_id),
                    daemon=True
                )
                hilo_riego.start()

    def leer_teclado(self):
        """Permite simular incidencias manuamente desde la consola."""
        while self.ejecutando:
            try:
                entrada = input().strip().lower()
                if entrada == 'ko':
                    with self.lock:
                        self.estado_salud = "KO"
                        self.regando = False
                    print(f"\n[ALERTA ENGINE {self.ws_id}] Estado cambiado a 'KO' (Fuga/Avería). Riego abortado.")
                elif entrada == 'ok':
                    with self.lock:
                        self.estado_salud = "OK"
                    print(f"\n[INFO ENGINE {self.ws_id}] Estado restablecido a 'OK'.")
                elif entrada == 'stop':
                    with self.lock:
                        if self.regando:
                            self.regando = False
                            print(f"\n[INFO ENGINE {self.ws_id}] Deteniendo el riego manualmente...")
                        else:
                            print(f"\n[INFO ENGINE {self.ws_id}] No hay ningún riego activo para detener.")
            except (EOFError, KeyboardInterrupt):
                break

    def iniciar(self):
        """Inicia el Engine y mantiene el bucle de reconexión Socket hacia el Monitor."""
        # Si se le pasó ws_id por parámetro al arrancar, habilitamos Kafka desde el inicio
        if self.ws_id and not self.hilo_kafka_iniciado:
            hilo_kafka = threading.Thread(target=self.escuchar_ordenes_kafka, daemon=True)
            hilo_kafka.start()
            self.hilo_kafka_iniciado = True

        threading.Thread(target=self.leer_teclado, daemon=True).start()

        id_mostrar = self.ws_id if self.ws_id else "esperando asignación..."
        print(f"\n=== ENGINE ACTIVADO (Estación: {id_mostrar}) ===")
        print("Comandos disponibles en consola:")
        print("  'ko'   -> Simular avería/fuga (aborta riego)")
        print("  'ok'   -> Restablecer a estado normal")
        print("  'stop' -> Detener riego actual manualmente")
        print("  Ctrl+C -> Apagar módulo\n")

        while self.ejecutando:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.connect((self.ip_monitor, self.puerto_monitor))
                self.responder_ping_monitor(sock)
            except (socket.error, ConnectionRefusedError):
                time.sleep(3)
            finally:
                sock.close()


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Uso: python WM_WS_E.py <ip_kafka> <puerto_kafka> <ip_monitor> <puerto_monitor> [<ws_id>]")
        sys.exit(1)

    ip_kafka = sys.argv[1]
    puerto_kafka = sys.argv[2]
    ip_monitor = sys.argv[3]
    puerto_monitor = int(sys.argv[4])
    ws_id = sys.argv[5] if len(sys.argv) > 5 else None

    engine = WateringStationEngine(ip_kafka, puerto_kafka, ip_monitor, puerto_monitor, ws_id)
    try:
        engine.iniciar()
    except KeyboardInterrupt:
        print("\n[ENGINE] Apagado ordenado por el usuario.")
        sys.exit(0)