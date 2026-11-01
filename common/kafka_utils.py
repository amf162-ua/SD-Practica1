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

Resiliencia: si el broker se cae, ni 'escuchar' ni 'publicar' lanzan excepciones.
Avisan una vez de forma controlada, reintentan solos y el módulo sigue funcionando
en lo que no depende de Kafka. Cuando el broker vuelve, continúan sin reiniciar nada.
"""

import json
import socket
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

REINTENTO = 3           # s entre reintentos cuando el broker no responde
MAX_BLOQUEO_MS = 2000   # máximo que un send() puede quedarse esperando al broker
# La librería espera hasta 30 s entre reintentos de reconexión; lo limitamos a REINTENTO
# para que, cuando el broker vuelva, todo se recupere en pocos segundos
MAX_ESPERA_RECONEXION_MS = REINTENTO * 1000


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
            # max_block_ms: si el broker cae, send() no se queda colgado 60 s (valor por defecto)
            productor = KafkaProducer(bootstrap_servers=servidor, value_serializer=_a_json,
                                      key_serializer=lambda k: k.encode('utf-8') if k else None,
                                      linger_ms=5, max_block_ms=MAX_BLOQUEO_MS,
                                      reconnect_backoff_max_ms=MAX_ESPERA_RECONEXION_MS)
            log(f"[KAFKA] Productor conectado a {servidor}")
            return productor
        except Exception as e:
            log(f"[KAFKA] Broker {servidor} no disponible ({type(e).__name__}). Reintento en {REINTENTO}s...")
            time.sleep(REINTENTO)


def publicar(productor, topic: str, clave: str, valor: dict, log=print) -> bool:
    """
    Publica un mensaje sin lanzar nunca una excepción (así no muere el hilo que lo llama).
    Devuelve False si no se ha podido entregar al productor porque el broker no responde.
    """
    if productor is None:
        return False
    try:
        futuro = productor.send(topic, key=clave, value=valor)
    except Exception as e:
        log(f"[KAFKA] Imposible publicar en {topic}: broker no disponible ({type(e).__name__})")
        return False
    # El envío real es asíncrono: si al final falla, también se avisa (sin traceback)
    futuro.add_errback(lambda e: log(f"[KAFKA] Mensaje a {topic} no entregado ({type(e).__name__})"))
    return True


def _poll(consumidor, timeout_ms):
    """poll() que devuelve los mensajes como lista plana."""
    return [msg for registros in consumidor.poll(timeout_ms=timeout_ms).values() for msg in registros]


def _broker_accesible(servidor: str) -> bool:
    """¿Acepta conexiones el broker? (abre y cierra una conexión TCP a host:puerto)."""
    host, puerto = servidor.rsplit(':', 1)
    try:
        socket.create_connection((host, int(puerto)), timeout=2).close()
        return True
    except OSError:
        return False


def escuchar(servidor: str, topics: list, grupo: str, log=print, listo=None):
    """
    Generador que devuelve (topic, valor) de los mensajes nuevos de los topics.
    'listo' (threading.Event opcional) se activa cuando el consumidor ya tiene
    particiones asignadas: a partir de ese momento no se pierde ningún mensaje.

    Si el broker se cae, el generador no termina ni lanza excepciones: sigue intentándolo
    con el mismo consumidor y, cuando el broker vuelve, continúa desde donde lo dejó.
    Para avisar de la caída y de la recuperación no nos fiamos de las excepciones de la
    librería (a veces no lanza ninguna): cada REINTENTO s se comprueba si el broker
    acepta conexiones.
    """
    while True:
        try:
            consumidor = KafkaConsumer(*topics, bootstrap_servers=servidor, group_id=grupo,
                                       value_deserializer=_de_json, auto_offset_reset='latest',
                                       reconnect_backoff_max_ms=MAX_ESPERA_RECONEXION_MS)
            break
        except Exception as e:
            log(f"[KAFKA] No se pudo suscribir ({type(e).__name__}). Reintento en {REINTENTO}s...")
            time.sleep(REINTENTO)

    suscrito = False       # ya tiene particiones asignadas
    caido = False          # para avisar solo una vez de la caída y una vez de la recuperación
    ultima_comprobacion = 0
    while True:
        try:
            if suscrito:
                mensajes = _poll(consumidor, 1000)
            else:
                # Hasta que poll() no asigna particiones, 'latest' no fija desde dónde leer
                mensajes = _poll(consumidor, 500)
                if consumidor.assignment():
                    suscrito = True
                    log(f"[KAFKA] Suscrito a {', '.join(topics)}")
                    if listo:
                        listo.set()
        except Exception:
            # Broker caído o reconectando: no dejamos que la excepción mate el hilo
            mensajes = []
            time.sleep(REINTENTO)

        if time.time() - ultima_comprobacion >= REINTENTO:
            ultima_comprobacion = time.time()
            accesible = _broker_accesible(servidor)
            if not accesible and not caido:
                log(f"[KAFKA] Conexión con el broker perdida. Reintentando cada {REINTENTO}s...")
                caido = True
            elif accesible and caido:
                log("[KAFKA] Conexión con el broker recuperada")
                caido = False

        for msg in mensajes:
            if msg.value is not None:
                yield msg.topic, msg.value
