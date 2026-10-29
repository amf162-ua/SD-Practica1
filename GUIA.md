# Guía del proyecto: cómo funciona y cómo probarlo en casa

Esta guía explica **qué hace cada parte del código** y **cómo levantar todo el sistema en un solo PC**
para probar todas las funcionalidades. Para el despliegue real en AWS + laboratorio, ver el
[README](README.md#guía-de-despliegue).

**Índice**
1. [Qué es cada cosa](#1-qué-es-cada-cosa)
2. [Cómo funciona el código](#2-cómo-funciona-el-código)
3. [Requisitos](#3-requisitos)
4. [Puesta en marcha en casa](#4-puesta-en-marcha-en-casa)
5. [Batería de pruebas](#5-batería-de-pruebas)
6. [Apagar](#6-apagar)
7. [Problemas frecuentes](#7-problemas-frecuentes)

---

## 1. Qué es cada cosa

| Programa | Simula | En la evaluación corre en |
|---|---|---|
| **WM_Central** ([WM_Central/](WM_Central/)) | La central del ayuntamiento: decide, guarda en la BD y muestra el panel | AWS |
| **WM_WS_M** (Monitor, [WM_WS/WM_WS_M.py](WM_WS/WM_WS_M.py)) | El vigilante de la estación: se autentica en CENTRAL y comprueba la salud del Engine | PC LAB 2 |
| **WM_WS_E** (Engine, [WM_WS/WM_WS_E.py](WM_WS/WM_WS_E.py)) | La parte física: electroválvula y caudalímetro | PC LAB 2 |
| **WM_FO** ([WM_FO/WM_FO.py](WM_FO/WM_FO.py)) | La app del jardinero para pedir riegos | PC LAB 1 |
| **Kafka** | El "tablón de anuncios" por el que viajan los mensajes | AWS |

Una **estación (WS)** son dos programas: Monitor + Engine.

Los programas se comunican de **dos formas**:

- **Sockets** (Monitor ↔ CENTRAL y Engine ↔ Monitor). Es una conexión directa y permanente entre dos.
  Sirve para saber **si el otro está vivo**: si se cae, el socket falla o deja de responder.
  Por eso la autenticación y la salud de la estación van por sockets.
- **Kafka** (todo lo demás). Uno publica un mensaje en un **tópico** y lo leen todos los suscritos.
  Nadie necesita conocer la IP de nadie, solo la del broker. Por eso las peticiones, órdenes,
  eventos y telemetría van por Kafka: los módulos quedan **desacoplados**.

```text
 WM_FO ──Kafka──►┐                       ┌──Kafka── WM_WS_E ◄──sockets──► WM_WS_M
                 │       WM_Central      │                                   │
 WM_FO ◄──Kafka──┤   (BD + panel web)    ├──Kafka──►                         │
                 └───────────▲───────────┘                                   │
                             └─────────────────────sockets───────────────────┘
```

---

## 2. Cómo funciona el código

### 2.1 Código común — [common/](common/)

#### Protocolo de sockets — [common/protocolo.py](common/protocolo.py)

TCP es un **chorro de bytes**, no de mensajes: si se envían "HOLA" y "ADIOS", pueden llegar como
"HOLAAD" + "IOS". Por eso cada mensaje se empaqueta así:

```text
<STX> D A T O S <ETX> <LRC>
0x02             0x03  XOR de todos los bytes de DATOS
```

- **STX/ETX** marcan inicio y fin. **LRC** detecta si el mensaje llegó corrupto.
- El receptor responde **ACK** (correcto) o **NACK** (corrupto → el emisor reenvía, hasta 3 veces).
- La conexión empieza con **ENQ → ACK** y termina con **EOT**.
- Los campos van separados por `#`, por ejemplo `AUTH#WS_01#Parque Norte#OK`.

La clase `CanalTramas` envuelve el socket. Guarda lo recibido en un **buffer** y de ahí extrae
mensajes completos, aunque lleguen partidos o pegados. Métodos principales:

| Método | Qué hace |
|---|---|
| `enviar(msg)` | Empaqueta, envía y espera ACK (reintenta si NACK) |
| `recibir()` | Lee una trama, valida el LRC, responde ACK/NACK y devuelve el texto |
| `peticion(msg)` | `enviar` + `recibir`: pregunta y devuelve la respuesta |
| `saludar()` / `esperar_saludo()` | ENQ/ACK al abrir la conexión |

El **timeout** del socket sirve para detectar que el otro no responde.

Mensajes que se intercambian por sockets:

| Origen → destino | Petición | Respuesta |
|---|---|---|
| Monitor → CENTRAL | `AUTH#<ws_id>#<ubicación>#<OK\|KO>` | `OK#<ws_id>` o `KO#ID_YA_CONECTADO` |
| Monitor → CENTRAL | `ESTADO#<ws_id>#<OK\|KO>` (al cambiar y cada 2 s como latido) | `OK` |
| Engine → Monitor | `HOLA` | `ID#<ws_id>` (el Monitor le asigna su ID) |
| Monitor → Engine | `SALUD` (cada segundo) | `OK` o `KO` |

#### Kafka — [common/kafka_utils.py](common/kafka_utils.py)

Hay **4 tópicos**:

| Tópico | Publica | Lee | Contenido |
|---|---|---|---|
| `wm-solicitudes` | FO, Engine (riego manual) | CENTRAL | Peticiones de riego |
| `wm-central` | CENTRAL | Engines, FO | Todo lo que sale de CENTRAL, según el campo `tipo` |
| `wm-eventos-ws` | Engine | CENTRAL | Engine listo, riego iniciado/rechazado/finalizado |
| `wm-telemetria` | Engine | CENTRAL, FO | Caudal y volumen cada segundo |

En `wm-central` el campo `tipo` indica para quién es cada mensaje:
- `ORDEN`: REGAR, PARAR, BLOQUEAR, ACTIVAR. Lo usa el Engine con ese `ws_id`.
- `NOTIFICACION`: cada paso de la solicitud y el resumen final. El `subtipo` es INFO,
  AUTORIZADA, DENEGADA o RESUMEN. Lo usa el FO con ese `order_id`.
- `ESTADO_RED`: el estado de todas las estaciones cada 2 s. Lo usa el FO.

Cada módulo ignora los mensajes que no son para él.

- `crear_productor` y `escuchar` **reintentan cada 3 s** si Kafka no está disponible, así que los
  módulos se pueden arrancar en cualquier orden.
- Cada módulo usa un **grupo de consumidor nuevo** en cada arranque y solo lee mensajes nuevos.
  Así, tras un reinicio, nunca reprocesa solicitudes antiguas.
- Los mensajes son JSON y llevan el `ws_id` como clave, de modo que los de una misma estación llegan en orden.

#### Estados — [common/estados.py](common/estados.py)

| Estado | Color en el panel | Cuándo |
|---|---|---|
| DISPONIBLE | Verde | Conectada, sana, no bloqueada y sin regar |
| REGANDO | Verde parpadeando | Riego en curso |
| FUGA | Rojo | El Engine responde KO o no responde |
| FUERA_SERVICIO | Naranja | Bloqueada por CENTRAL |
| DESCONECTADA | Gris | El Monitor no está conectado a CENTRAL |

### 2.2 CENTRAL — [WM_Central/](WM_Central/)

**El estado se deduce, no se guarda.** Cada `Estacion` guarda hechos: `conectada`, `salud`,
`bloqueada`, `riego` (el riego en curso) y `pendiente` (una solicitud esperando al Engine).
El estado se calcula a partir de ellos, por prioridad:

```python
if not conectada:  DESCONECTADA
if salud == KO:    FUGA
if bloqueada:      FUERA_SERVICIO
if riego:          REGANDO
else:              DISPONIBLE
```

**Hilos que trabajan a la vez** (por eso se protege todo con `self.lock`):
1. `bucle_sockets`: acepta Monitores y crea **un hilo por Monitor** (`atender_monitor`).
2. `bucle_kafka`: procesa solicitudes, eventos y telemetría.
3. `bucle_estado_red`: publica cada 2 s el estado de la red para los operarios.
4. El panel web ([panel_web.py](WM_Central/panel_web.py) + [panel.html](WM_Central/panel.html)) en `http://<ip>:8080`.

**Cuando llega una solicitud de riego** (`procesar_solicitud`):
1. Notifica *"Solicitud recibida"*.
2. Comprueba que el operario existe en la BD, que la estación existe, que está DISPONIBLE y que no
   tiene otra solicitud en curso. Si algo falla → **DENEGADA** con el motivo.
3. Si todo está bien, reserva la estación, envía **REGAR** al Engine y arranca un temporizador de 5 s.
   Si el Engine no contesta en ese tiempo, deniega.

**Cuando llega un evento del Engine** (`procesar_evento`):
- `RIEGO_INICIADO` → notifica **AUTORIZADA**.
- `RIEGO_FINALIZADO` → guarda el riego en la BD y envía el **RESUMEN** al operario.
- `ENGINE_LISTO` → le recuerda al Engine si está bloqueado.

**Sockets con el Monitor** (`atender_monitor`): `AUTH` registra la estación (si es nueva la da de alta)
y `ESTADO#...#KO` la marca con fuga y corta el riego. Si en **6 s** no llega nada (el Monitor manda un
latido cada 2 s), la estación pasa a DESCONECTADA.

**Órdenes del punto 11** (`orden_central`, botones del panel): REGAR, BLOQUEAR y ACTIVAR, sobre una
estación o sobre todas.

**Base de datos** ([database.py](WM_Central/database.py)): tablas `estaciones` (id, ubicación, bloqueada),
`operarios` (`OP_01`…`OP_04` por defecto) y `riegos` (histórico). Lo que depende de conexiones vivas
(conectada, regando) no se guarda. Por eso, tras un reinicio, todas salen **DESCONECTADAS** hasta
que se reconectan.

### 2.3 Monitor — [WM_WS/WM_WS_M.py](WM_WS/WM_WS_M.py)

Es **cliente** de CENTRAL y **servidor** del Engine. Tiene dos hilos:
- **Con CENTRAL**: se conecta y envía `AUTH`. Después manda `ESTADO` cada vez que cambia la salud y
  cada 2 s como latido. Si CENTRAL se cae, reintenta cada 3 s.
- **Con el Engine**: espera su conexión, le asigna el ID y cada segundo le pregunta `SALUD`.
  Si responde **KO**, **no responde en 3 s** o se cae la conexión, la salud pasa a KO (avería/fuga).

La salud empieza en KO: mientras no haya un Engine respondiendo, la estación no es segura.

### 2.4 Engine — [WM_WS/WM_WS_E.py](WM_WS/WM_WS_E.py)

Tiene tres hilos:
- **Con el Monitor**: recibe su ID y responde a cada `SALUD`. Si pierde al Monitor, corta el riego.
- **Kafka**: lee de `wm-central` las `ORDEN` de su `ws_id` (REGAR, PARAR, BLOQUEAR, ACTIVAR).
- **Menú** de la estación (`r`, `p`, `k`, `o`, `e`; ver la sección 4).

**El riego** se ejecuta en otro hilo (`_regar`). Cada segundo simula el caudalímetro
(~15 L/min), suma el volumen y publica la telemetría. Para al cumplirse la duración o cuando alguien
pone un motivo de parada (fuga, parada manual, bloqueo, desconexión). Al terminar publica
`RIEGO_FINALIZADO` con el motivo, los segundos y los litros.

### 2.5 Operario — [WM_FO/WM_FO.py](WM_FO/WM_FO.py)

- Escucha `wm-central` (sus notificaciones y el estado de la red) y `wm-telemetria` (la de su riego).
- `solicitar` publica la petición y **espera** con dos `threading.Event`: `decidida`
  (AUTORIZADA o DENEGADA, máximo 15 s) y `terminada` (llega el RESUMEN).
- Con un fichero de servicios ([servicios.txt](WM_FO/servicios.txt), formato `WS_ID segundos`) pide
  cada servicio, espera a que termine y hace una **pausa de 4 s** antes del siguiente.

### 2.6 Un riego completo, mensaje a mensaje

```text
FO      ──wm-solicitudes──►  CENTRAL   {order_id, operator_id: OP_01, ws_id: WS_01, duracion: 10}
CENTRAL ──wm-central──────►  FO        NOTIFICACION/INFO "Solicitud recibida..."
        (comprueba operario y estación → la reserva)
CENTRAL ──wm-central──────►  Engine    ORDEN REGAR
Engine  ──wm-eventos-ws───►  CENTRAL   RIEGO_INICIADO
CENTRAL ──wm-central──────►  FO        NOTIFICACION/AUTORIZADA          (panel: verde parpadeando)
Engine  ──wm-telemetria───►  CENTRAL + FO   caudal y volumen, cada segundo × 10
Engine  ──wm-eventos-ws───►  CENTRAL   RIEGO_FINALIZADO (COMPLETADO, 2.5 L)
CENTRAL ──wm-central──────►  FO        NOTIFICACION/RESUMEN              (panel: verde)
```

Si a mitad de riego se pulsa `k` en el Engine: el Engine corta el riego, el Monitor recibe `KO` en
el siguiente `SALUD` y lo reporta a CENTRAL, y la estación pasa a ROJO. El operario recibe el resumen
con motivo FUGA.

---

## 3. Requisitos

- **Docker Desktop** abierto y con *Engine running*. Todo corre en contenedores: **no hace falta
  instalar Python ni Kafka**.
- **Git** y **VS Code**, con varias terminales abiertas (`Ctrl+ñ`, botón **+** para abrir más y el
  icono de dividir para verlas lado a lado).
- Bajar la última versión del repositorio:
  ```bash
  git pull
  ```

---

## 4. Puesta en marcha en casa

En casa, un solo PC hace de AWS y de los dos PCs del laboratorio. Cada parte corre en sus propios
contenedores, igual que en la evaluación.

### 4.0 Configurar la IP (la primera vez y cada vez que cambies de red)

1. Averigua tu IP con `ipconfig` → *Adaptador de LAN inalámbrica Wi-Fi* (o *Ethernet*) → *Dirección IPv4*.
   Por ejemplo, `192.168.1.50`.
2. Crea los ficheros `.env` a partir de los ejemplos. **Git no los sube**, así que cada uno tiene los suyos:
   ```bash
   cp deploy/cloud/.env.example deploy/cloud/.env
   ```
   ```bash
   cp deploy/lab/.env.example deploy/lab/.env
   ```
3. Edítalos y pon **tu IP** en lugar de `1.2.3.4`:
   - `deploy/cloud/.env` → `KAFKA_PUBLIC_HOST=192.168.1.50`
   - `deploy/lab/.env` → `CENTRAL_HOST=192.168.1.50` y `KAFKA_HOST=192.168.1.50`

> ⚠️ **No pongas `localhost`.** Los contenedores de las estaciones y operarios no llegarían a Kafka.
> Si cambias de red Wi-Fi tu IP cambia: actualiza los dos `.env` y repite los `up -d`.

### ¿Desde qué carpeta se lanza cada comando?

| Comando | Desde |
|---|---|
| `docker compose ...` de la parte cloud | `deploy/cloud` |
| `docker compose -f compose.ws.yml ...` / `-f compose.fo.yml ...` | `deploy/lab` |
| `docker logs`, `docker attach`, `docker start/stop/restart`, `docker run` | Cualquier carpeta |

Lo más cómodo:
- **Terminal 1**: en `deploy/cloud`.
- **Terminal 2**: en `deploy/lab`.
- **El resto**: sin `cd`, solo para ver pantallas.

### 4.1 Parte Cloud: Kafka + CENTRAL (terminal 1)

```bash
cd deploy/cloud
```
```bash
docker compose up -d --build
```
La primera vez tarda un par de minutos. Comprueba que todo está en marcha:
```bash
docker compose ps
```
✅ `broker` → `Up (healthy)` y `wm-central` → `Up`.

- **Panel de CENTRAL**: abre **http://localhost:8080** en el navegador y déjalo visible todo el rato.
- **Log de CENTRAL** (en una terminal aparte):
  ```bash
  docker logs -f wm-central
  ```

### 4.2 Estaciones: PC LAB 2 (terminal 2)

```bash
cd deploy/lab
```
```bash
docker compose -f compose.ws.yml up -d --build
```
✅ En unos 10 s, **WS_01** y **WS_02** aparecen en **verde** en el panel.

### 4.3 Operarios: PC LAB 1 (terminal 2)

⚠️ Arráncalos **cuando las estaciones ya estén en verde**: `fo2` ejecuta el fichero de servicios nada
más arrancar.

```bash
docker compose -f compose.fo.yml up -d --build
```

### 4.4 Ver las pantallas

Abre una terminal para cada una:

| Pantalla | Comando | Tipo |
|---|---|---|
| CENTRAL | `docker logs -f wm-central` | Solo lectura |
| Monitor de WS_01 | `docker logs -f ws1-monitor` | Solo lectura |
| Engine de WS_01 | `docker attach ws1-engine` | **Interactiva** (menú) |
| Engine de WS_02 | `docker attach ws2-engine` | **Interactiva** (menú) |
| Operario OP_01 | `docker attach fo1` | **Interactiva** (menú) |
| Operario OP_02 | `docker logs -f fo2` | Ejecuta [servicios.txt](WM_FO/servicios.txt) |

**Uso de `docker attach` (importante):**
- Al entrar parece que no pasa nada: solo se ve lo que ocurre a partir de ese momento. **Pulsa Enter**
  y se muestra el menú.
- Para salir **sin apagar** el programa: **Ctrl+P y luego Ctrl+Q**.
- **Ctrl+C lo apaga.** Si te pasa, vuelve a encenderlo con `docker start ws1-engine` (o el nombre que sea).

**Menú del Engine:**

| Tecla | Acción |
|---|---|
| `r 10` | Pedir a CENTRAL un riego manual de 10 s en esta estación |
| `p` | Parar el riego en curso |
| `k` | Simular una avería/fuga (el Engine responde KO al Monitor) |
| `o` | Avería resuelta (vuelve a responder OK) |
| `e` | Ver el estado de la estación |
| Enter | Mostrar el menú y el estado |

**Menú del operario:** `1` pedir riego · `2` ver estaciones · `3` ejecutar un fichero de servicios · `0` salir.

---

## 5. Batería de pruebas

Hazlas en orden: algunas dependen de la anterior. En cada una mira el **panel**, el **log de
CENTRAL** y la pantalla del módulo implicado. Aprovecha para sacar **capturas para la memoria**.

### A. Arranque y registro
| # | Qué hacer | Resultado esperado |
|---|---|---|
| A1 | Nada (ya está arrancado) | CENTRAL: `[AUTH] WS_01 autenticada`. Monitor: `Autenticado en CENTRAL` y `Engine conectado`. Engine: `ID asignado: WS_01` |
| A2 | Panel → *Alta de estación*: `WS_05` / `Parque Lo Morant` | WS_05 aparece en **gris** (dada de alta, sin conectar) |

### B. Fichero de servicios (fo2)
| # | Qué hacer | Resultado esperado |
|---|---|---|
| B1 | Mira `docker logs -f fo2` | 3 riegos con *Solicitud recibida* → *Solicitando apertura* → *AUTORIZADA* → telemetría cada segundo → **RESUMEN**, y *Esperando 4 s* entre servicios |
| B2 | — | El último servicio (WS_99) sale **DENEGADA: La estación WS_99 no existe** |
| B3 | `docker restart fo2` y vuelve a hacer `docker logs -f fo2` | Se repite la ejecución |

Se puede editar [servicios.txt](WM_FO/servicios.txt) y hacer `docker restart fo2` sin reconstruir nada.

### C. Riego desde el operario (fo1)
| # | Qué hacer | Resultado esperado |
|---|---|---|
| C1 | fo1: `2` | Lista de todas las estaciones con su estado |
| C2 | fo1: `1` → `WS_01` → `20` | fo1: pasos y telemetría. Panel: WS_01 **verde parpadeando** con caudal, volumen, tiempo y operario. Engine: `ELECTROVÁLVULA ABIERTA` y una línea por segundo |
| C3 | Esperar a que termine | fo1: **RESUMEN** (COMPLETADO, 20 s, ~5 L). Panel: verde y una fila nueva en *Últimos riegos* |

### D. Estación ocupada
| # | Qué hacer | Resultado esperado |
|---|---|---|
| D1 | Panel: elige WS_01, duración 30, **Iniciar riego** | WS_01 regando (operario CENTRAL) |
| D2 | Mientras riega, fo1: `1` → `WS_01` → `5` | **DENEGADA: La estación no está disponible (estado: REGANDO)** |

### E. Fuga
| # | Qué hacer | Resultado esperado |
|---|---|---|
| E1 | Con WS_01 regando, en su Engine escribe `k` | Engine: `FUGA simulada` y `ELECTROVÁLVULA CERRADA (FUGA)`. Monitor: `Salud -> KO`. Panel: **ROJO**. El operario recibe el resumen con motivo FUGA |
| E2 | fo1 pide riego en WS_01 | **DENEGADA** (estado: FUGA) |
| E3 | En el Engine escribe `o` | Monitor: `Salud -> OK`. Panel: vuelve a **verde** |

### F. Riego y parada manual desde la estación
| # | Qué hacer | Resultado esperado |
|---|---|---|
| F1 | Engine: `r 30` | La petición pasa por CENTRAL. Panel: regando, operario `WS_01-LOCAL` |
| F2 | Engine: `p` | Para al momento. *Últimos riegos* muestra **PARADA_MANUAL** |

### G. Órdenes de CENTRAL (punto 11 del enunciado)
| # | Qué hacer | Resultado esperado |
|---|---|---|
| G1 | Panel: WS_02, 30 s, **Iniciar riego** | WS_02 regando |
| G2 | Panel: destino *Todas las estaciones* → **Bloquear** | Todas en **NARANJA "Fuera de Servicio"**. El riego de WS_02 se corta (BLOQUEO_CENTRAL). Engines: `FUERA DE SERVICIO por orden de CENTRAL` |
| G3 | fo1 pide riego en WS_01; Engine: `r 5` | Ambos **DENEGADA** (FUERA_SERVICIO) |
| G4 | Panel: *Todas* → **Activar** | Todas en **verde** |
| G5 | Panel: *Todas* → **Iniciar riego** (10 s) | Todas las conectadas riegan a la vez |

### H. Operarios
| # | Qué hacer | Resultado esperado |
|---|---|---|
| H1 | `docker run --rm -it wm-fo python -u WM_FO/WM_FO.py <tu IP> 9092 OP_99` → `1` → `WS_01` → `5` | **DENEGADA: El operario OP_99 no está dado de alta en CENTRAL**. Salir con `0` |
| H2 | Panel → *Operarios*: alta de `OP_99` / `Prueba`, y repetir H1 | Ahora se autoriza |

### I. Resiliencia (caídas)
| # | Qué hacer | Resultado esperado |
|---|---|---|
| I1 | Con WS_01 regando: `docker stop ws1-engine` | En ~3 s el Monitor dice `El Engine no responde`, el panel pasa a **ROJO**, el riego se cierra y el operario recibe el resumen |
| I2 | `docker start ws1-engine` | Vuelve a **verde** solo |
| I3 | `docker stop ws1-monitor` | En ≤ 6 s, panel **GRIS** |
| I4 | `docker start ws1-monitor` | Se reconecta y vuelve a verde |
| I5 | Bloquea WS_02 desde el panel y después `docker restart wm-central` | CENTRAL arranca con `N estaciones cargadas de la BD (DESCONECTADAS)`. Las estaciones se reconectan solas y **WS_02 sigue bloqueada** (está guardado en la BD). Luego, actívala |
| I6 | `docker stop broker`, esperar 10 s, `docker start broker` | Los módulos muestran `Reintento en 3s...` y siguen funcionando |

### J. Estación nueva e ID duplicado
| # | Qué hacer | Resultado esperado |
|---|---|---|
| J1 | Lanzar una tercera estación con los dos comandos de abajo | **WS_03** aparece en el panel; si no existía, se da de alta sola |
| J2 | `docker run --rm wm-ws python -u WM_WS/WM_WS_M.py 6000 <tu IP> 5000 WS_01 Duplicada` | CENTRAL: **Rechazada WS_01: ya hay una estación conectada con ese ID**. Salir con Ctrl+C |

Comandos de J1. Primero el Monitor:
```bash
docker run --rm -d --name ws3-monitor --network wm-ws_default wm-ws python -u WM_WS/WM_WS_M.py 6000 <tu IP> 5000 WS_03 Plaza de los Luceros
```
y después el Engine (deja abierto su menú):
```bash
docker run --rm -it --name ws3-engine --network wm-ws_default wm-ws python -u WM_WS/WM_WS_E.py <tu IP> 9092 ws3-monitor 6000
```
Al terminar, Ctrl+C en el Engine y `docker stop ws3-monitor`.

---

## 6. Apagar

Desde `deploy/lab`:
```bash
docker compose -f compose.fo.yml down
```
```bash
docker compose -f compose.ws.yml down
```
Desde `deploy/cloud`:
```bash
docker compose down
```
La base de datos se conserva en un volumen de Docker. Para **empezar de cero** (BD vacía), apaga con:
```bash
docker compose down -v
```

---

## 7. Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| Estaciones siempre en gris y el Monitor dice `timed out` | La IP de los `.env` no es la actual. Revisa `ipconfig`, corrige los dos `.env` y repite los `up -d` |
| `Broker ... no disponible` en el FO o el Engine | Kafka aún está arrancando (espera) o `KAFKA_PUBLIC_HOST` tiene una IP incorrecta |
| `docker compose` dice que falta `KAFKA_PUBLIC_HOST` | No existe `deploy/cloud/.env` → ver sección 4.0 |
| `docker attach` se queda en blanco | Es normal: pulsa Enter para ver el menú |
| He cerrado un Engine/FO con Ctrl+C | `docker start <nombre>` y vuelve a hacer `docker attach <nombre>` |
| He cambiado código y no se nota | Falta `--build` en el `docker compose up` |
| `The container name "/broker" is already in use` | Queda un contenedor antiguo: `docker rm -f broker` y repite |
