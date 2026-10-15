import sys
import time
import json
import threading
from kafka import KafkaConsumer, KafkaProducer

def deserializar_json(mensaje_bytes):
    """Convierte los bytes recibidos de Kafka en un diccionario JSON."""
    if mensaje_bytes is None:
        return None
    return json.loads(mensaje_bytes.decode('utf-8'))


class FieldOperator:
    def __init__(self, ip_kafka, puerto_kafka, operator_id):
        self.bootstrap_server = f"{ip_kafka}:{puerto_kafka}"
        self.operator_id = operator_id
        self.producer = None
        self.consumer = None
        
        # Variables de sincronización para órdenes en curso
        self.orden_activa = None
        self.ws_activa = None
        self.evento_orden_terminada = threading.Event()

        self._iniciar_productor()
        self._iniciar_hilo_consumidor()

    def _iniciar_productor(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_server,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"[FO {self.operator_id}] Productor Kafka conectado a {self.bootstrap_server}")
        except Exception as e:
            print(f"[ERROR FO] No se pudo conectar productor Kafka: {e}")
            sys.exit(1)

    def _iniciar_hilo_consumidor(self):
        def _escuchar_kafka():
            while self.consumer is None:
                try:
                    self.consumer = KafkaConsumer(
                        'wm-orders-response',
                        'wm-telemetry',
                        bootstrap_servers=self.bootstrap_server,
                        value_deserializer=deserializar_json,
                        group_id=f'operator-group-{self.operator_id}-{int(time.time())}',
                        auto_offset_reset='latest'
                    )
                except Exception:
                    time.sleep(3)

            for msg in self.consumer:
                topico = msg.topic
                datos = msg.value

                # 1. Respuestas de autorización o resumen final
                if topico == 'wm-orders-response':
                    if datos.get("operator_id") == self.operator_id:
                        # Caso A: Respuesta a la solicitud
                        if "status" in datos and "event" not in datos:
                            estado = datos.get("status")
                            razon = datos.get("reason", "")
                            if estado == "AUTHORIZED":
                                print(f"\n[AUTORIZADO] CENTRAL ha aprobado el riego en {datos.get('ws_id')}.")
                                print("Iniciando suministro... Recibiendo telemetría en tiempo real:\n")
                            else:
                                print(f"\n[DENEGADO] CENTRAL ha rechazado la solicitud. Motivo: {razon}\n")
                                self.evento_orden_terminada.set()

                        # Caso B: Resumen final emitido por Central
                        elif datos.get("event") == "WATERING_SUMMARY":
                            print("\n" + "="*50)
                            print(f"      RESUMEN FINAL DE RIEGO - ESTACIÓN {datos.get('ws_id')}")
                            print("="*50)
                            print(f"  Operario:     {datos.get('operator_id')}")
                            print(f"  Estado Final: {datos.get('status')}")
                            print(f"  Duración:     {datos.get('duration_seconds')} segundos")
                            print(f"  Volumen:      {datos.get('total_volume_l')} Litros consumidos")
                            print("="*50 + "\n")
                            self.evento_orden_terminada.set()

                # 2. Telemetría en directo solo si la estación que riega es la nuestra
                elif topico == 'wm-telemetry':
                    if self.ws_activa and datos.get("ws_id") == self.ws_activa:
                        caudal = datos.get("flow_rate_lmin")
                        acumulado = datos.get("accumulated_volume_l")
                        seg = datos.get("elapsed_seconds")
                        print(f"[{self.ws_activa}] T={seg}s | Caudal: {caudal} L/min | Acumulado: {acumulado} L")

        hilo = threading.Thread(target=_escuchar_kafka, daemon=True)
        hilo.start()

    def solicitar_riego(self, ws_id, duracion_segundos):
        """Envía la petición a Central y espera a que concluya el servicio."""
        self.orden_activa = f"ORD-{int(time.time())}"
        self.ws_activa = ws_id
        self.evento_orden_terminada.clear()

        peticion = {
            "order_id": self.orden_activa,
            "operator_id": self.operator_id,
            "ws_id": ws_id,
            "duration_seconds": duracion_segundos
        }

        print(f"\n[ENVIANDO] Solicitando riego para {ws_id} ({duracion_segundos}s)...")
        self.producer.send('wm-orders-request', key=ws_id.encode('utf-8'), value=peticion)
        self.producer.flush()

        # Esperar hasta que concluya (sea autorizada+terminada, o rechazada) con margen de seguridad
        concluido = self.evento_orden_terminada.wait(timeout=duracion_segundos + 10)
        if not concluido:
            print(f"\n[AVISO FO] Tiempo límite de espera agotado ({duracion_segundos + 10}s). La orden no pudo completarse.")
        self.ws_activa = None
        self.orden_activa = None

    def procesar_fichero(self, ruta_fichero):
        """Lee un archivo con formato: <WS_ID> <DURACION> y ejecuta las órdenes con pausa de 4s."""
        print(f"\n[MODO FICHERO] Leyendo servicios desde: {ruta_fichero}")
        try:
            with open(ruta_fichero, 'r', encoding='utf-8') as f:
                lineas = [l.strip() for l in f if l.strip() and not l.startswith('#')]
        except Exception as e:
            print(f"[ERROR] No se pudo leer el archivo: {e}")
            return

        for idx, linea in enumerate(lineas):
            partes = linea.split()
            if len(partes) < 2:
                continue
            ws_id = partes[0]
            duracion = int(partes[1])

            print(f"\n--- Servicio {idx+1}/{len(lineas)}: {ws_id} ({duracion}s) ---")
            self.solicitar_riego(ws_id, duracion)

            # Requisito PDF (Página 11): Esperar 4 segundos tras finalizar cada servicio
            if idx < len(lineas) - 1:
                print("[PAUSA] Esperando 4 segundos antes de solicitar el siguiente servicio...")
                time.sleep(4)

        print("\n[MODO FICHERO] Todos los servicios del archivo han sido completados.")

    def modo_interactivo(self):
        """Menú por terminal para uso manual por parte del operario."""
        print(f"\n=== APLICACIÓN DE OPERARIO DE CAMPO ({self.operator_id}) ===")
        while True:
            try:
                print("\nOpciones:")
                print("1. Solicitar riego en una estación")
                print("2. Salir")
                opcion = input("Selecciona una opción (1-2): ").strip()

                if opcion == '1':
                    ws_id = input("ID de la estación (ej. WS_01): ").strip()
                    duracion_str = input("Duración en segundos (ej. 10): ").strip()
                    if ws_id and duracion_str.isdigit():
                        self.solicitar_riego(ws_id, int(duracion_str))
                    else:
                        print("[ERROR] Parámetros no válidos.")
                elif opcion == '2':
                    print("Saliendo de la aplicación del operario...")
                    break
            except (KeyboardInterrupt, EOFError):
                break


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: python WM_FO.py <ip_kafka> <puerto_kafka> <id_operador> [<fichero_servicios>]")
        sys.exit(1)

    ip_kafka = sys.argv[1]
    puerto_kafka = sys.argv[2]
    id_operador = sys.argv[3]
    fichero = sys.argv[4] if len(sys.argv) > 4 else None

    operario = FieldOperator(ip_kafka, puerto_kafka, id_operador)
    time.sleep(1) # Tiempo de estabilización con Kafka

    if fichero:
        operario.procesar_fichero(fichero)
    else:
        operario.modo_interactivo()
