# Guía del proyecto — cómo funciona y por qué

1. [El sistema en una imagen](#1-el-sistema-en-una-imagen)
2. [Sockets o Kafka: quién usa qué y por qué](#2-sockets-o-kafka-quién-usa-qué-y-por-qué)
3. [Protocolo de sockets](#3-protocolo-de-sockets)
4. [Kafka: tópicos y grupos](#4-kafka-tópicos-y-grupos)
5. [Los módulos por dentro](#5-los-módulos-por-dentro)
6. [Un riego, mensaje a mensaje](#6-un-riego-mensaje-a-mensaje)
7. [Qué pasa si cae cada pieza](#7-qué-pasa-si-cae-cada-pieza)
8. [Preguntas que nos pueden hacer](#8-preguntas-que-nos-pueden-hacer)

---

## 1. El sistema en una imagen

```text
          AWS (EC2)                                  PC estaciones
 ┌───────────────────────────┐   sockets :5000   ┌──────────────────────────┐
 │  WM_Central ── SQLite     │◄─────────────────►│ WM_WS_M (Monitor)        │
 │  panel web :8080          │                   │    ▲ sockets :6000       │
 │                           │                   │    ▼                     │
 │  Kafka :9092 ◄────────────┼───────────────────┤ WM_WS_E (Engine)         │
 └───────────▲───────────────┘      Kafka        └──────────────────────────┘
             │ Kafka
     ┌───────┴────────┐  PC operarios
     │ WM_FO (fo1,fo2)│
     └────────────────┘
```

| Módulo | Qué simula | Fichero |
|---|---|---|
| **WM_Central** | La central: decide, guarda en la BD y muestra el panel | [WM_Central/WM_Central.py](WM_Central/WM_Central.py) |
| **WM_WS_M** (Monitor) | El vigilante de la estación: se autentica y comprueba la salud del Engine | [WM_WS/WM_WS_M.py](WM_WS/WM_WS_M.py) |
| **WM_WS_E** (Engine) | La parte física: electroválvula y caudalímetro | [WM_WS/WM_WS_E.py](WM_WS/WM_WS_E.py) |
| **WM_FO** | La app del jardinero | [WM_FO/WM_FO.py](WM_FO/WM_FO.py) |
| `common/` | Código compartido: protocolo, Kafka y estados | [common/](common/) |


**Parámetros** (nada fijo en el código; los composes los toman del `.env`):

| Módulo | Argumentos |
|---|---|
| WM_Central | `<puerto_sockets> <ip_kafka> [<puerto_kafka>] [--panel 8080] [--db ruta]` |
| WM_WS_M | `<puerto_engine> <ip_central> <puerto_central> <ws_id> [<ubicación>] [--registrar]` |
| WM_WS_E | `<ip_kafka> <puerto_kafka> <ip_monitor> <puerto_monitor>` (el ID se lo da el Monitor) |
| WM_FO | `<ip_kafka> <puerto_kafka> <id_operador> [<fichero_servicios>]` |

---

## 2. Sockets o Kafka: quién usa qué y por qué

| Enlace | Tecnología | Por qué |
|---|---|---|
| Monitor ↔ CENTRAL | **Sockets** | Conexión directa y permanente: si el Monitor cae, CENTRAL lo nota (se rompe el socket o deja de llegar el latido). Ideal para autenticar y vigilar la salud. |
| Engine ↔ Monitor | **Sockets** | Igual: el Monitor necesita saber al segundo si el Engine responde. |
| Todo lo demás | **Kafka** | Desacopla: nadie conoce la IP de nadie, solo la del broker. Un mensaje lo pueden leer varios (la telemetría la leen CENTRAL y el FO) y Kafka lo **guarda**, así que no se pierde si el destinatario está caído. |

---

## 3. Protocolo de sockets

[common/protocolo.py](common/protocolo.py). Cada mensaje viaja como:
```text
<STX> D A T O S <ETX> <LRC>      STX=0x02 · ETX=0x03 · LRC = XOR de todos los bytes de DATOS
```
- El receptor recalcula el LRC: **ACK** si coincide; **NACK** si no, y el emisor reenvía (hasta 3 veces).
- La conexión empieza con **ENQ → ACK** y termina con **EOT**. Los campos van separados por `#`.
- **Buffer:** TCP es un chorro de bytes, y un `recv()` puede traer medio mensaje o dos pegados. `CanalTramas` acumula lo recibido y extrae mensajes completos.

| Origen → destino | Petición | Respuesta |
|---|---|---|
| Monitor → CENTRAL | `REGISTRO#<ws>#<ubicación>` (solo con `--registrar`) | `OK#<ws>` · `OK#YA_REGISTRADA` |
| Monitor → CENTRAL | `AUTH#<ws>#<ubicación>#<OK\|KO>` | `OK#<ws>` · `KO#WS_NO_REGISTRADA` · `KO#ID_YA_CONECTADO` |
| Monitor → CENTRAL | `ESTADO#<ws>#<OK\|KO>` (al cambiar y cada 2 s = latido) | `OK` |
| Engine → Monitor | `HOLA` | `ID#<ws>` (el Monitor le asigna el ID) |
| Monitor → Engine | `SALUD` (cada 1 s) | `OK` · `KO` |

**Tiempos clave:** salud cada **1 s** · Engine sin responder **3 s** → avería · latido a CENTRAL cada **2 s** · CENTRAL sin latido **6 s** → *Disconnected*.

---

## 4. Kafka: tópicos y grupos

[common/kafka_utils.py](common/kafka_utils.py)

| Tópico | Publica → lee | Contenido |
|---|---|---|
| `wm-solicitudes` | FO, Engine (manual) → CENTRAL | Peticiones de riego |
| `wm-central` | CENTRAL → Engine, FO | Según `tipo`: **ORDEN** (REGAR, PARAR, BLOQUEAR, ACTIVAR), **NOTIFICACION** (pasos y resumen), **ESTADO_RED** (todas las WS cada 2 s) |
| `wm-eventos-ws` | Engine → CENTRAL | ENGINE_LISTO, RIEGO_INICIADO / RECHAZADO / FINALIZADO |
| `wm-telemetria` | Engine → CENTRAL, FO | Caudal y volumen cada segundo |

**4 tópicos = 4 canales:** lo que entra a CENTRAL, lo que sale de CENTRAL, y lo que publican las estaciones, separando eventos puntuales de telemetría continua. Cada consumidor descarta lo que no es suyo (por `tipo`, `ws_id` u `order_id`). La clave de cada mensaje es el `ws_id`, así los de una misma estación llegan en orden.

**Grupos de consumidores.** Kafka guarda los mensajes (12 h) y recuerda **por dónde va cada grupo** (el *offset*).

| Módulo | Grupo | Por qué |
|---|---|---|
| CENTRAL | Fijo `wm-central` | Al volver de una caída recibe lo que se perdió (p. ej. un riego que terminó mientras estaba caída) |
| FO | Fijo `fo-<operario>-<máquina>` | Al reabrirse ve el resultado de sus servicios. La máquina va en el nombre porque dos apps en el mismo grupo se repartirían los mensajes |
| Engine | **Nuevo** en cada arranque | Un Engine que se reinicia **no** debe ejecutar un REGAR antiguo |

Para no ejecutar peticiones viejas, al arrancar se mira dónde termina cada tópico: los mensajes anteriores llegan marcados como **atrasados** y CENTRAL **descarta las solicitudes atrasadas** (quien las pidió ya dejó de esperar).

**Si Kafka cae**, nadie se rompe: `escuchar` y `publicar` capturan los errores, avisan una vez (*Conexión con el broker perdida / recuperada*) y reintentan cada 3 s. Lo publicado durante la caída espera en el productor hasta 2 minutos.

---

## 5. Los módulos por dentro

### CENTRAL
- **El estado se deduce, no se guarda.** Cada estación guarda hechos (`conectada`, `salud`, `bloqueada`, `riego`) y el color sale de ellos por prioridad:
  ```text
  no conectada → Disconnected (gris) · salud KO → Leak (rojo) · bloqueada → Out of Service (naranja)
  · riego en curso → Watering (verde parpadeando) · resto → Available (verde)
  ```
  Así nunca hay estados incoherentes.
- **Hilos:** uno por cada Monitor conectado (sockets), uno para Kafka, uno que publica el estado de la red cada 2 s y el panel web. Comparten las estaciones, así que todo pasa por un candado (`RLock`).
- **Solicitud de riego:** comprueba operario (BD), estación, que esté *Available* y que no tenga otra petición. Si todo va bien, la **reserva**, manda REGAR y espera 5 s a que el Engine confirme. Si no confirma → DENEGADA.
- **Riegos idempotentes:** antes de guardar mira si ese `order_id` ya está, porque tras una caída un mismo fin de riego puede llegar dos veces.
- **BD (SQLite):** `estaciones` (id, ubicación, bloqueada), `operarios` y `riegos`. "Conectada" y "regando" **no** se guardan: dependen de conexiones vivas. Por eso al reiniciar todas salen grises.
- **Panel:** la página pide `GET /api/estado` cada segundo. Los botones llaman a `POST /api/orden`, `/api/estaciones` y `/api/operarios`.

### Monitor
- **Cliente** de CENTRAL: `AUTH` y luego `ESTADO` (latido). Si CENTRAL cae: *Imposible conectar con WM_Central* y reintenta cada 3 s.
- **Servidor** del Engine: le da el ID y le pregunta `SALUD` cada segundo. Si responde KO, no responde en 3 s o se corta la conexión → salud KO.
- Empieza en KO: sin un Engine respondiendo, la estación no es segura.

### Engine
- **Riego** en su propio hilo: cada segundo simula el caudalímetro (~15 L/min), suma litros y publica la telemetría. Para al cumplir la duración o con un motivo (fuga, parada manual, bloqueo, desconexión) y publica RIEGO_FINALIZADO.
- **No pierde lo regado:** cada segundo guarda el riego en curso en `datos/riego_en_curso_<WS>.json` (en un volumen de Docker). Si el Engine muere, al volver encuentra el fichero y envía el estado final con motivo **AVERIA**.
- Sin Monitor queda **AVERIADA** y rechaza riegos, también los manuales.

### Operario (FO)
- Pide un riego y espera con dos `threading.Event`: **decidida** (AUTORIZADA o DENEGADA, máx. 15 s) y **terminada** (RESUMEN).
- Solo muestra lo de **su** pedido (`order_id`), pero recibe el estado de **todas** las WS (opción 2).
- **Vigila a CENTRAL:** si en 6 s no llega su estado de la red (que sale cada 2 s), avisa *Imposible conectar con WM_Central* y no envía peticiones hasta que vuelva.
- Fichero de servicios: `WS_ID segundos` por línea; espera a que acabe cada uno y 4 s más.

---

## 6. Un riego, mensaje a mensaje

```text
FO      ─wm-solicitudes─►  CENTRAL   {order_id, OP_01, WS_01, 10 s}
CENTRAL ─wm-central─────►  FO        NOTIFICACION INFO "Solicitud recibida"
CENTRAL ─wm-central─────►  Engine    ORDEN REGAR                (CENTRAL reserva la WS, espera 5 s)
Engine  ─wm-eventos-ws──►  CENTRAL   RIEGO_INICIADO
CENTRAL ─wm-central─────►  FO        NOTIFICACION AUTORIZADA    (panel: verde parpadeando)
Engine  ─wm-telemetria──►  CENTRAL+FO caudal y volumen, cada segundo
Engine  ─wm-eventos-ws──►  CENTRAL   RIEGO_FINALIZADO (COMPLETADO, 2.5 L)
CENTRAL ─wm-central─────►  FO        NOTIFICACION RESUMEN       (guardado en la BD; panel: verde)
```
Mientras tanto, por sockets: Monitor → Engine `SALUD` cada segundo, y Monitor → CENTRAL `ESTADO` cada 2 s.

---

## 7. Qué pasa si cae cada pieza

| Cae | Cómo se detecta | Qué pasa | Al volver |
|---|---|---|---|
| **Monitor** | CENTRAL pierde el socket o el latido (≤ 6 s) | Gris. CENTRAL cierra el riego y avisa al FO. El Engine corta el riego, queda AVERIADA y rechaza riegos manuales | Reconecta solo (cada 3 s) → verde |
| **Engine** | El Monitor pierde la conexión o no tiene respuesta (≤ 3 s) | Rojo. CENTRAL guarda el riego con la última telemetría y avisa al FO | Lee su fichero y envía el estado final → la BD se corrige con motivo AVERIA |
| **FO** | — | El riego sigue | Grupo fijo: recibe lo que se perdió → *RESULTADO DE UN SERVICIO ANTERIOR* |
| **CENTRAL** | Monitor: socket. FO: 6 s sin estado de la red | Los riegos en curso terminan. No se admiten peticiones (el FO avisa y no envía) | Carga la BD (gris → verde), registra los riegos que terminaron, **adopta** los que siguen en curso y descarta las peticiones viejas |
| **Kafka** | Comprobación cada 3 s | Avisos controlados, sin errores. Los sockets siguen funcionando | Todo continúa en segundos sin reiniciar nada |

---

## 8. Preguntas que nos pueden hacer

- **¿Por qué un buffer en los sockets?** Porque TCP no respeta los límites de los mensajes: un `recv()` puede traer medio mensaje o dos.
- **¿Para qué sirve el LRC?** Para detectar mensajes corruptos. Con NACK, el emisor reenvía.
- **¿Cómo sabe CENTRAL que una WS se ha caído si la red muere sin cerrar el socket?** Por el latido: 2 s de intervalo y 6 s de margen.
- **¿Por qué hay un candado en CENTRAL?** Porque varios hilos (uno por Monitor, Kafka, panel) modifican las mismas estaciones a la vez.
- **¿Qué pasa si dos WS usan el mismo ID?** La segunda recibe `KO#ID_YA_CONECTADO`.
- **¿Y una WS no registrada?** `KO#WS_NO_REGISTRADA`. Se da de alta desde el panel o arrancando el Monitor con `--registrar`.
- **¿Qué pasa si dos operarios piden la misma WS a la vez?** El primero la reserva y al segundo se le deniega.
- **¿Por qué el Engine no usa un grupo fijo y CENTRAL sí?** CENTRAL no debe perder eventos; el Engine no debe ejecutar órdenes viejas.
- **¿Cómo evitáis guardar un riego dos veces?** Antes de guardar se comprueba el `order_id` (idempotencia).
- **¿Por qué hay que poner la IP pública en `.env`?** Kafka anuncia esa dirección a los clientes. Si es otra, conectan una vez y luego fallan.
- **¿Cómo se escala?** Más WS o más FO = más contenedores con otro ID. Cada uno se conecta solo y no afecta al resto.
