"""
Tópicos y utilidades de Kafka compartidas.

Tópico           Productor           Consumidores   Contenido
---------------  ------------------  -------------  ------------------------------------------------
wm-solicitudes   FO, WS_E (manual)   CENTRAL        Peticiones de riego
wm-central       CENTRAL             WS_E, FO       Todo lo que sale de CENTRAL, según el campo 'tipo':
                                                      ORDEN         -> REGAR / PARAR / BLOQUEAR / ACTIVAR (WS_E)
                                                      NOTIFICACION  -> pasos de la solicitud y resumen (FO, WS_E)
                                                      ESTADO_RED    -> foto de todas las WS cada 2 s (FO)
wm-eventos-ws    WS_E                CENTRAL        Engine listo / riego iniciado, rechazado o finalizado
wm-telemetria    WS_E                CENTRAL, FO    Caudal y volumen cada segundo

Cada consumidor se queda solo con los mensajes que le interesan (por 'tipo',
ws_id u order_id) y descarta el resto.
Todos los mensajes son JSON y usan el ws_id como clave, así los mensajes de una
misma estación van a la misma partición y se consumen en orden.
"""

import json
import time
import uuid

import warnings

warnings.filterwarnings('ignore', category=DeprecationWarning)
from kafka import KafkaConsumer, KafkaProducer  # noqa: E402

TOPIC_SOLICITUDES = 'wm-solicitudes'
TOPIC_CENTRAL = 'wm-central'
TOPIC_EVENTOS_WS = 'wm-eventos-ws'
TOPIC_TELEMETRIA = 'wm-telemetria'

# Valores del campo 'tipo' de los mensajes de wm-central
TIPO_ORDEN = 'ORDEN'
TIPO_NOTIFICACION = 'NOTIFICACION'
TIPO_ESTADO_RED = 'ESTADO_RED'


def bootstrap(ip: str, puerto) -> str:
    """Admite 'ip puerto' por separado o 'ip:puerto' en el primer argumento."""
    return ip if ':' in ip else f"{ip}:{puerto}"


def nuevo_id(prefijo: str) -> str:
    return f"{prefijo}-{uuid.uuid4().hex[:8]}"


def _a_json(valor) -> bytes:
    return json.dumps(valor).encode('utf-8')


def _de_json(datos: bytes):
    try:
        return json.loads(datos.decode('utf-8'))
    except (ValueError, AttributeError):
        return None


def crear_productor(servidor: str, log=print) -> KafkaProducer:
    """Crea un productor, reintentando hasta que el broker esté disponible."""
    while True:
        try:
            productor = KafkaProducer(bootstrap_servers=servidor, value_serializer=_a_json,
                                      key_serializer=lambda k: k.encode('utf-8') if k else None,
                                      linger_ms=5)
            log(f"[KAFKA] Productor conectado a {servidor}")
            return productor
        except Exception as e:
            log(f"[KAFKA] Broker {servidor} no disponible ({e}). Reintento en 3s...")
            time.sleep(3)


def escuchar(servidor: str, topics: list, grupo: str, log=print, listo=None):
    """
    Generador que devuelve (topic, valor) de los mensajes nuevos de los topics.
    'listo' (threading.Event opcional) se activa cuando el consumidor ya tiene
    particiones asignadas: a partir de ese momento no se pierde ningún mensaje.
    """
    while True:
        try:
            consumidor = KafkaConsumer(*topics, bootstrap_servers=servidor, group_id=grupo,
                                       value_deserializer=_de_json, auto_offset_reset='latest')
            break
        except Exception as e:
            log(f"[KAFKA] No se pudo suscribir ({e}). Reintento en 3s...")
            time.sleep(3)

    # Hasta que poll() no asigna particiones, 'latest' no fija desde dónde leer
    pendientes = []
    while not consumidor.assignment():
        for registros in consumidor.poll(timeout_ms=500).values():
            pendientes.extend(registros)
    log(f"[KAFKA] Suscrito a {', '.join(topics)}")
    if listo:
        listo.set()

    for msg in pendientes:
        if msg.value is not None:
            yield msg.topic, msg.value
    for msg in consumidor:
        if msg.value is not None:
            yield msg.topic, msg.value
