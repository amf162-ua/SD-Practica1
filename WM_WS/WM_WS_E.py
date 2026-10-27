import socket
import sys
import time
import threading
import json
import os
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from kafka import KafkaConsumer, KafkaProducer

DIRECTORIO_ACTUAL = os.path.dirname(os.path.abspath(__file__))
if DIRECTORIO_ACTUAL not in sys.path:
    sys.path.insert(0, DIRECTORIO_ACTUAL)

from protocolo import empaquetar, desempaquetar, ACK, NACK, ENQ, EOT


def deserializar_json(mensaje_bytes):
    """Convierte bytes de Kafka en dict Python."""
    if mensaje_bytes is None:
        return None
    try:
        return json.loads(mensaje_bytes.decode('utf-8'))
    except Exception:
        return None


class WateringStationEngine:
    """
    Módulo Engine de la Estación de Riego (WM_WS_E).

    Responsabilidades:
    - Conectarse al Monitor (WM_WS_M) mediante socket TCP local.
    - Responder a los PINGs de salud del Monitor (OK / KO).
    - Escuchar en Kafka ('wm-orders-response') las autorizaciones de riego de Central.
    - Simular la electroválvula: emitir telemetría cada segundo en 'wm-telemetry'.
    - Publicar el evento de fin de riego en 'wm-alerts-events'.
    - Permitir simulación manual de incidencias desde consola (ko / ok / stop).
    """

    def __init__(self, ip_kafka: str, puerto_kafka: str,
                 ip_monitor: str, puerto_monitor: int,
                 ws_id: str = None):
        # Soporte para bootstrap_servers completo (host:puerto) o separado
        self.bootstrap_kafka = ip_kafka if ":" in str(ip_kafka) else f"{ip_kafka}:{puerto_kafka}"
        self.ip_monitor      = ip_monitor
        self.puerto_monitor  = int(puerto_monitor)
        self.ws_id           = ws_id

        # Estado interno protegido con Lock (acceso concurrente entre hilos)
        self.lock           = threading.Lock()
        self.estado_salud   = "OK"   # 'OK' o 'KO' (fuga/avería)
        self.regando        = False

        # Productor Kafka único (reutilizado para telemetría y eventos)
        self.producer           = None
        self.ejecutando         = True
        self.hilo_kafka_iniciado = False

    def _crear_producer(self):
        """Inicializa el productor Kafka compartido para telemetría y alertas."""
        while self.producer is None and self.ejecutando:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_kafka,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print(f"[ENGINE {self.ws_id}] Productor Kafka conectado a {self.bootstrap_kafka}")
            except Exception as e:
                print(f"[ENGINE {self.ws_id}] Error conectando productor Kafka ({e}). Reintentando...")
                time.sleep(3)

    def responder_ping_monitor(self, sock):
        """
        Atiende los mensajes del Monitor a través del socket interno:
          - CONFIG#<ws_id> → asigna el ID y arranca el consumidor Kafka
          - PING           → responde con el estado de salud actual (OK / KO)
        """
        try:
            while self.ejecutando:
                trama = sock.recv(1024)
                if not trama:
                    break

                if trama == ENQ:
                    sock.send(ACK)
                    continue

                if trama == EOT:
                    break

                mensaje, valido = desempaquetar(trama)
                if not valido:
                    sock.send(NACK)
                    continue
                sock.send(ACK)  # Confirmar recepción correcta de la trama

                if mensaje.startswith("CONFIG#"):
                    # Asignación del ID por parte del Monitor
                    self.ws_id = mensaje.split("#")[1]
                    print(f"[ENGINE] ID asignado por el Monitor: {self.ws_id}")

                    # Iniciar Kafka ahora que conocemos el ws_id
                    if not self.hilo_kafka_iniciado:
                        threading.Thread(target=self._iniciar_kafka, daemon=True).start()
                        self.hilo_kafka_iniciado = True

                    sock.send(empaquetar("OK#ID_RECIBIDO"))
                    ack = sock.recv(1024)  # Esperar ACK del Monitor

                elif mensaje == "PING":
                    # Responder con el estado de salud actual
                    with self.lock:
                        estado = self.estado_salud
                    sock.send(empaquetar(estado))
                    ack = sock.recv(1024)  # Esperar ACK del Monitor

        except Exception as e:
            print(f"[ENGINE {self.ws_id}] Conexión cerrada con Monitor: {e}")
        finally:
            # Seguridad: si se pierde la conexión con el Monitor, abortar el riego
            with self.lock:
                if self.regando:
                    self.regando = False
                    print(f"\n[ENGINE {self.ws_id}] ¡Conexión con Monitor perdida! Riego abortado por seguridad.")

    def _iniciar_kafka(self):
        """Arranca el productor compartido y el consumidor de órdenes."""
        self._crear_producer()
        self.escuchar_ordenes_kafka()

    def publicar_telemetria_kafka(self, duracion_segundos: int = 10,
                                   caudal_lmin: float = 15.0,
                                   operator_id: str = "DESCONOCIDO"):
        """
        Simula el riego:
        - Abre la electroválvula (self.regando = True).
        - Emite telemetría de caudal y volumen acumulado cada segundo en 'wm-telemetry'.
        - Finaliza cuando: se cumple el tiempo, se llama 'stop', o el estado es 'KO'.
        - Publica el evento de cierre en 'wm-alerts-events' con el resumen completo.
        """
        with self.lock:
            if self.regando:
                print(f"[ENGINE {self.ws_id}] Ya hay un riego en curso. Orden ignorada.")
                return
            self.regando = True

        print(f"\n[ENGINE {self.ws_id}] ELECTROVÁLVULA ABIERTA — Riego iniciado ({duracion_segundos}s) por {operator_id}")

        volumen_acumulado   = 0.0
        segundos_transcurridos = 0

        while segundos_transcurridos < duracion_segundos:
            with self.lock:
                if not self.regando or self.estado_salud != "OK":
                    break

            time.sleep(1)
            segundos_transcurridos += 1
            volumen_acumulado += caudal_lmin / 60.0  # L/min → L/s

            payload = {
                "ws_id":                self.ws_id,
                "flow_rate_lmin":       caudal_lmin,
                "accumulated_volume_l": round(volumen_acumulado, 2),
                "elapsed_seconds":      segundos_transcurridos
            }

            try:
                self.producer.send('wm-telemetry', key=self.ws_id.encode('utf-8'), value=payload)
                print(f"[ENGINE {self.ws_id}] T={segundos_transcurridos}s | "
                      f"Caudal: {caudal_lmin} L/min | Acumulado: {round(volumen_acumulado, 2)} L")
            except Exception as e:
                print(f"[ENGINE {self.ws_id}] Error al enviar telemetría: {e}")

        with self.lock:
            self.regando = False
            estado_final = self.estado_salud

        print(f"[ENGINE {self.ws_id}] ELECTROVÁLVULA CERRADA — Riego finalizado.\n")

        # Determinar motivo de finalización
        if estado_final != "OK":
            motivo = "ABORTED_LEAK"
        elif segundos_transcurridos >= duracion_segundos:
            motivo = "COMPLETED"
        else:
            motivo = "STOPPED_MANUAL"

        # Publicar evento de fin de riego
        resumen = {
            "event":            "WATERING_FINISHED",
            "ws_id":            self.ws_id,
            "operator_id":      operator_id,
            "status":           motivo,
            "duration_seconds": segundos_transcurridos,
            "total_volume_l":   round(volumen_acumulado, 2)
        }

        try:
            self.producer.send('wm-alerts-events', key=self.ws_id.encode('utf-8'), value=resumen)
            self.producer.flush()
            print(f"[ENGINE {self.ws_id}] Resumen enviado a 'wm-alerts-events': {motivo}\n")
        except Exception as e:
            print(f"[ENGINE {self.ws_id}] Error al enviar resumen: {e}\n")

    def escuchar_ordenes_kafka(self):
        """
        Consumidor Kafka: escucha 'wm-orders-response' para recibir autorizaciones de riego.
        Solo procesa mensajes dirigidos a esta estación (ws_id coincidente) con status AUTHORIZED.
        """
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
            except Exception as e:
                print(f"[ENGINE {self.ws_id}] Error conectando consumidor Kafka ({e}). Reintentando...")
                time.sleep(3)

        for msg in consumer:
            if not self.ejecutando:
                break
            datos = msg.value
            if not datos:
                continue

            # Filtrar solo órdenes para esta estación que estén autorizadas
            if datos.get("ws_id") == self.ws_id and datos.get("status") == "AUTHORIZED":
                duracion    = datos.get("duration_seconds", 10)
                operator_id = datos.get("operator_id", "DESCONOCIDO")

                threading.Thread(
                    target=self.publicar_telemetria_kafka,
                    args=(duracion, 15.0, operator_id),
                    daemon=True
                ).start()

    def leer_teclado(self):
        """
        Permite simular incidencias desde la consola en tiempo de ejecución (PDF, pág. 11):
          ko   → Simula una fuga/avería (aborta el riego si está activo)
          ok   → Restablece el estado a normal
          stop → Detiene el riego manual (sin avería)
        """
        while self.ejecutando:
            try:
                entrada = input().strip().lower()
                if entrada == 'ko':
                    with self.lock:
                        self.estado_salud = "KO"
                        self.regando = False
                    print(f"\n[ENGINE {self.ws_id}] Avería/Fuga simulada. Estado → KO. Riego abortado.")
                elif entrada == 'ok':
                    with self.lock:
                        self.estado_salud = "OK"
                    print(f"\n[✓ ENGINE {self.ws_id}] Estado restablecido → OK.")
                elif entrada == 'stop':
                    with self.lock:
                        if self.regando:
                            self.regando = False
                            print(f"\n[ENGINE {self.ws_id}] Riego detenido manualmente.")
                        else:
                            print(f"\n[ENGINE {self.ws_id}] No hay ningún riego activo.")
            except (EOFError, KeyboardInterrupt):
                break

    def iniciar(self):
        """
        Punto de entrada del Engine:
        1. Si se proporcionó ws_id en la línea de comandos, inicia Kafka directamente.
        2. Lanza el hilo de lectura de teclado (ko/ok/stop).
        3. Entra en el bucle de reconexión hacia el Monitor.
        """
        # Si conocemos el ws_id desde el inicio, arrancar Kafka sin esperar al Monitor
        if self.ws_id and not self.hilo_kafka_iniciado:
            threading.Thread(target=self._iniciar_kafka, daemon=True).start()
            self.hilo_kafka_iniciado = True

        threading.Thread(target=self.leer_teclado, daemon=True).start()

        print(f"\n=== ENGINE ACTIVADO (Estación: {self.ws_id or 'pendiente de asignación'}) ===")
        print("Comandos en consola:")
        print("  ko   → Simular avería/fuga (aborta riego)")
        print("  ok   → Restablecer estado normal")
        print("  stop → Detener riego manual")
        print("  Ctrl+C → Apagar módulo\n")

        # Bucle de reconexión al Monitor (reintenta automáticamente si cae)
        while self.ejecutando:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                print(f"[ENGINE] Conectando al Monitor ({self.ip_monitor}:{self.puerto_monitor})...")
                sock.connect((self.ip_monitor, self.puerto_monitor))
                print(f"[ENGINE] Conectado al Monitor.")
                self.responder_ping_monitor(sock)
            except (socket.error, ConnectionRefusedError):
                print(f"[ENGINE] Monitor no disponible. Reintentando en 3s...")
                time.sleep(3)
            finally:
                sock.close()


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Uso: python WM_WS_E.py <ip_kafka> <puerto_kafka> <ip_monitor> <puerto_monitor> [<ws_id>]")
        print("  Ejemplo: python WM_WS_E.py 192.168.1.5 9092 127.0.0.1 6001 WS_01")
        sys.exit(1)

    ip_kafka      = sys.argv[1]
    puerto_kafka  = sys.argv[2]
    ip_monitor    = sys.argv[3]
    puerto_monitor = int(sys.argv[4])
    ws_id         = sys.argv[5] if len(sys.argv) > 5 else None

    engine = WateringStationEngine(ip_kafka, puerto_kafka, ip_monitor, puerto_monitor, ws_id)
    try:
        engine.iniciar()
    except KeyboardInterrupt:
        print("\n[ENGINE] Apagado ordenado.")
        sys.exit(0)