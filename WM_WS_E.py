import socket
import sys
import time
import threading
import json
from kafka import KafkaConsumer, KafkaProducer
from protocolo import empaquetar, desempaquetar, ACK, NACK, ENQ, EOT

FORMAT = 'utf-8'
estado_salud = "OK"
regando = False


def responder_ping_monitor(sock):
    """Mantiene el canal de presencia e incidencias locales con el Monitor por Sockets."""
    global estado_salud
    try:
        while True:
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
            
            if mensaje == "PING":
                respuesta = empaquetar(estado_salud)
                sock.send(respuesta)
                
                ack_recibido = sock.recv(1024)
                if ack_recibido != ACK:
                    pass
                    
    except Exception:
        pass


def publicar_telemetria_kafka(bootstrap_server, ws_id, duracion_segundos=10, caudal_lmin=15.0):
    """Bucle que simula el flujo de agua y publica telemetría cada 1 segundo en Kafka."""
    global regando, estado_salud
    regando = True
    print(f"\n[ENGINE {ws_id}] ELECTROVÁLVULA ABIERTA - Iniciando riego ({duracion_segundos}s)...")
    
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"[ENGINE {ws_id}] Error conectando productor Kafka: {e}")
        regando = False
        return

    volumen_acumulado = 0.0
    segundos_transcurridos = 0

    while segundos_transcurridos < duracion_segundos and regando and estado_salud == "OK":
        time.sleep(1)
        segundos_transcurridos += 1
        volumen_acumulado += (caudal_lmin / 60.0) # Convertir L/min a L/segundo

        payload = {
            "ws_id": ws_id,
            "flow_rate_lmin": caudal_lmin,
            "accumulated_volume_l": round(volumen_acumulado, 2),
            "elapsed_seconds": segundos_transcurridos
        }

        try:
            producer.send('wm-telemetry', key=ws_id.encode('utf-8'), value=payload)
            print(f"[ENGINE {ws_id}] Telemetría enviada -> Caudal: {caudal_lmin} L/min | Acumulado: {round(volumen_acumulado, 2)} L")
        except Exception as e:
            print(f"[ENGINE {ws_id}] Error al enviar telemetría: {e}")

    regando = False
    print(f"[ENGINE {ws_id}] ELECTROVÁLVULA CERRADA - Riego finalizado.\n")


def escuchar_ordenes_kafka(bootstrap_server, ws_id):
    """Escucha las respuestas de autorización en Kafka para activar el riego."""
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                'wm-orders-response',
                bootstrap_servers=bootstrap_server,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id=f'engine-group-{ws_id}',
                auto_offset_reset='latest'
            )
            print(f"[ENGINE {ws_id}] Escuchando órdenes en el tópico 'wm-orders-response'...")
        except Exception:
            time.sleep(3)

    for msg in consumer:
        datos = msg.value
        # Filtrar solo si el mensaje es para esta Estación
        if datos.get("ws_id") == ws_id and datos.get("status") == "AUTHORIZED":
            duracion = datos.get("duration_seconds", 10)
            # Iniciar el riego en un hilo aparte para no bloquear la escucha de Kafka
            hilo_riego = threading.Thread(
                target=publicar_telemetria_kafka,
                args=(bootstrap_server, ws_id, duracion),
                daemon=True
            )
            hilo_riego.start()


def iniciar_engine(ip_kafka, puerto_kafka, ip_monitor, puerto_monitor, ws_id):
    global estado_salud
    bootstrap_kafka = f"{ip_kafka}:{puerto_kafka}"

    # 1. Hilo para escuchar órdenes desde Kafka
    hilo_kafka = threading.Thread(
        target=escuchar_ordenes_kafka,
        args=(bootstrap_kafka, ws_id),
        daemon=True
    )
    hilo_kafka.start()

    # 2. Hilo para cambiar el estado manualmente vía consola (OK/KO)
    def leer_teclado():
        global estado_salud, regando
        while True:
            try:
                entrada = input().strip().lower()
                if entrada == 'ko':
                    estado_salud = "KO"
                    regando = False # Abortar riego activo si hay avería
                    print("\n[ALERTA ENGINE] Estado cambiado a 'KO' (Fuga/Avería). Riego abortado.")
                elif entrada == 'ok':
                    estado_salud = "OK"
                    print("\n[INFO ENGINE] Estado restablecido a 'OK'.")
            except EOFError:
                break

    threading.Thread(target=leer_teclado, daemon=True).start()

    print(f"\n=== ENGINE ACTIVADO (Estación: {ws_id}) ===")
    print("Escribe 'ko' para simular avería/fuga.")
    print("Escribe 'ok' para reparar y volver a estado normal.")
    print("Presiona Ctrl+C para apagar.\n")

    # 3. Bucle persistente de conexión Socket hacia el Monitor
    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((ip_monitor, puerto_monitor))
            responder_ping_monitor(sock)
        except (socket.error, ConnectionRefusedError):
            time.sleep(3)
        finally:
            sock.close()


if __name__ == "__main__":
    if len(sys.argv) < 6:
        print("Uso: python WM_WS_E.py <ip_kafka> <puerto_kafka> <ip_monitor> <puerto_monitor> <ws_id>")
        sys.exit(1)

    ip_kafka = sys.argv[1]
    puerto_kafka = sys.argv[2]
    ip_monitor = sys.argv[3]
    puerto_monitor = int(sys.argv[4])
    ws_id = sys.argv[5]

    try:
        iniciar_engine(ip_kafka, puerto_kafka, ip_monitor, puerto_monitor, ws_id)
    except KeyboardInterrupt:
        print("\n[ENGINE] Apagado limpio por usuario (Ctrl+C).")
        sys.exit(0)