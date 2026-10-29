# Memoria de Prácticas: Water Management Network (Release 1)
## Sistemas Distribuidos · Curso Académico 2026 / 2027

---

### Datos de los Estudiantes
- **Estudiante 1:** Àlex Morell Far — **DNI:** 54375989X
- **Estudiante 2:** Iker Roig Mengual — **DNI:** 74019728R
- **Titulación:** Grado en Ingeniería Informática
- **Centro:** Escuela Politécnica Superior (EPS), Universidad de Alicante
- **Asignatura:** Sistemas Distribuidos (SD)
- **Repositorio GitHub:** `UA_SD` / [amf162-ua/SD-Practica1](https://github.com/amf162-ua/SD-Practica1.git)

---

## 1. Introducción y Objetivos

### 1.1 Contexto y Problemática Real
El uso eficiente de los recursos hídricos es un desafío crítico para las ciudades inteligentes modernas. Las redes de riego urbano convencionales sufren ineficiencias notables debido a la falta de supervisión en tiempo real: roturas de tuberías o fugas en electroválvulas que pasan desapercibidas durante días, estaciones fuera de servicio sin diagnóstico centralizado y falta de correlación entre las órdenes de los operarios y el agua suministrada. Según datos del Instituto Nacional de Estadística (INE), las pérdidas en las redes de distribución en España rondan el 14,6% del volumen total, mientras que ciudades pioneras como Alicante alcanzan rendimientos hidráulicos del 88,55% gracias a la digitalización integral de su sensorización.

En este marco, la presente práctica desarrolla la **Release 1** de la plataforma distribuida **WaterManagement**, un sistema de control, telemetría y monitorización online en tiempo real capaz de gestionar una red distribuida de estaciones de riego (**Watering Stations - WS**) en parques y jardines, permitiendo su gobierno tanto desde una central (**WM_Central**) como por parte de operarios de mantenimiento (**Field Operators - FO**).

### 1.2 Objetivos Técnicos Alcanzados
1. **Integración Multiparadigma:** Coexistencia armónica de sockets TCP puros (comunicación directa orientada a conexión, control de presencia viva y bajo overhead) y streaming de eventos asíncrono con **Apache Kafka** (desacoplamiento espacial y temporal para órdenes, telemetría y eventos).
2. **Protocolo a Nivel de Enlace Propietario:** Definición e implementación estricta del protocolo de tramas delimitadas `<STX><DATA><ETX><LRC>` con cálculo de redundancia longitudinal (XOR byte a byte), apretón de manos `ENQ`/`ACK`, acuses de recibo positivos/negativos (`ACK`/`NACK`) y retransmisiones automáticas.
3. **Persistencia Transaccional y Recuperación:** Almacenamiento seguro en **SQLite** para el censo de estaciones, operarios autorizados e histórico de auditoría de riegos. Separación estricta entre estado volátil deducido en memoria y estado persistente.
4. **Alta Concurrencia y Sincronización:** Servidores multihilo concurrentes con exclusión mutua mediante cerrojos reentrantes (`threading.RLock`), eliminando condiciones de carrera.
5. **Resiliencia Extrema ante Fallos:** Aislamiento de fallos, tolerancia a caídas abruptas de cualquier componente (Central, Monitores, Engines, Operarios o Broker) y recuperación autónoma sin pérdida de datos ni transacciones.
6. **Despliegue Distribuido en Nube y Laboratorio con Docker:** Despliegue en contenedor de la parte Cloud en **AWS EC2 (Learner Lab)** integrada con integración continua (**GitHub Actions CI/CD**), y ejecución de estaciones y operarios en equipos independientes de laboratorio.

---

## 2. Arquitectura Global del Sistema

El sistema se estructura siguiendo una arquitectura en capas híbrida, donde los componentes se distribuyen físicamente entre la infraestructura Cloud y los equipos locales de campo.

```
                          ┌──────────────── AWS EC2 (Cloud Hosting) ────────────────┐
                          │                                                         │
   PC LAB 2 (Estaciones)  │    ┌───────────────────────────┐   ┌────────────────┐   │   PC LAB 1 (Operarios)
 ┌──────────────────────┐ │    │        WM_Central         │───│  SQLite (BD)   │   │  ┌────────────────────┐
 │  WM_WS_M (Monitor)   │─┼───►│  - Sockets Server :5000   │   │  /data/*.db    │   │  │  WM_FO (fo1)       │
 │   ▲ (Sockets :6000)  │ │    │  - Web Panel :8080        │   └────────────────┘   │  │  - Interactivo     │
 │   ▼                  │ │    └─────────────┬─────────────┘                        │  └─────────┬──────────┘
 │  WM_WS_E (Engine)    │─┼────────┐         │                                      │            │
 └──────────────────────┘ │        ▼         ▼                                      │            │
                          │   ┌─────────────────────────────────────────────────┐   │            ▼
                          │   │          Apache Kafka (Broker) :9092            │◄──┼────────────┘
                          │   │   wm-solicitudes | wm-central                   │   │  WM_FO (fo2)
                          │   │   wm-eventos-ws  | wm-telemetria                │   │  - Batch (servicios)
                          │   └─────────────────────────────────────────────────┘   │
                          └─────────────────────────────────────────────────────────┘
```

### 2.1 Justificación de los Mecanismos de Comunicación
- **Sockets TCP (Síncronos, Conexión Permanente):**
  - Utilizados entre `WM_WS_M` y `WM_Central`, y entre `WM_WS_E` y `WM_WS_M`.
  - *Justificación:* Permiten una vinculación directa entre pares. La rotura del socket o la ausencia de latido en tiempo prefijado identifica de forma inequívoca e instantánea la caída del componente físico sin depender de capas intermedias. Resulta óptimo para autenticación y supervisión de salud (*heartbeat*).
- **Apache Kafka (Streaming de Eventos Asíncrono):**
  - Utilizado para peticiones de operarios, envío de órdenes de riego, telemetría de caudal en tiempo real y notificaciones de resumen.
  - *Justificación:* Proporciona desacoplamiento total en el espacio (ningún operario necesita conocer la dirección IP de las estaciones ni de la Central) y en el tiempo (tolerancia ante latencias de red y picos de carga). El particionamiento por clave (`ws_id`) asegura el orden estricto de los eventos de cada estación.

---

## 3. Protocolos de Comunicación y Estructura de Mensajes

### 3.1 Protocolo Estándar de Sockets (`common/protocolo.py`)
Para superar la problemática intrínseca de TCP (flujo continuo de bytes no orientado a mensaje), se ha diseñado una capa de empaquetado y control de flujo sobre tramas bien formadas:

```
+-------+---------------------------------------+-------+-------+
|  STX  |     D A T A  (campos con '#')         |  ETX  |  LRC  |
| (0x02)|     CODIGO#campo1#...#campoN          | (0x03)| (XOR) |
+-------+---------------------------------------+-------+-------+
```

#### Reglas del Protocolo
1. **Control de Flujo:**
   - **Apertura:** El cliente transmite `ENQ` (0x05); el servidor valida y responde `ACK` (0x06).
   - **Transmisión de Trama:** Tras enviar `<STX>DATA<ETX><LRC>`, el emisor aguarda un byte de respuesta.
   - **Confirmación:** Si el receptor verifica que el cálculo del LRC coincide con el byte recibido, retorna `ACK`. Si el LRC no concuerda o la trama está corrupta, retorna `NACK` (0x15).
   - **Reintentos:** El emisor reintenta la transmisión hasta un máximo de 3 veces consecutivas antes de declarar error de protocolo.
   - **Cierre:** Terminación limpia mediante byte `EOT` (0x04).
2. **Cálculo del LRC (Longitudinal Redundancy Check):**
   $$LRC = \bigoplus_{i=1}^{n} Byte_i$$
   Implementado en Python mediante:
   ```python
   def calcular_lrc(data: bytes) -> int:
       lrc = 0
       for byte in data:
           lrc ^= byte
       return lrc
   ```
3. **Manejo del Buffer TCP en `CanalTramas`:**
   Dado que `sock.recv()` puede entregar tramas fragmentadas o múltiples tramas contiguas, `CanalTramas` acumula los datos en un buffer binario y delimita rigurosamente entre los marcadores `STX` y `ETX`, extrayendo el LRC subsiguiente.

#### Catálogo de Mensajes por Sockets
| Origen $\to$ Destino | Mensaje / Trama | Respuesta Esperada | Propósito |
|---|---|---|---|
| `WM_WS_M` $\to$ `WM_Central` | `REGISTRO#<ws_id>#<ubicación>` | `OK#<ws_id>` / `KO#YA_REGISTRADA` | Solicitud de alta censal desde campo. |
| `WM_WS_M` $\to$ `WM_Central` | `AUTH#<ws_id>#<ubicación>#<salud>` | `OK#<ws_id>` / `KO#WS_NO_REGISTRADA` / `KO#ID_YA_CONECTADO` | Autenticación y verificación de validez. |
| `WM_WS_M` $\to$ `WM_Central` | `ESTADO#<ws_id>#<OK\|KO>` | `OK` | Latido periódico (cada 2s) y notificación de fuga. |
| `WM_WS_E` $\to$ `WM_WS_M` | `HOLA` | `ID#<ws_id>` | Solicitud de identificación inicial al Monitor. |
| `WM_WS_M` $\to$ `WM_WS_E` | `SALUD` | `OK` / `KO` | Chequeo continuo de salud (cada 1s). |

---

### 3.2 Tópicos y Mensajería Kafka (`common/kafka_utils.py`)
El sistema utiliza 4 tópicos segmentados según la naturaleza del tráfico:

| Tópico | Emisores | Receptores | Clave (Key) | Contenido del Payload JSON |
|---|---|---|---|---|
| `wm-solicitudes` | `WM_FO`, `WM_WS_E` (riego manual) | `WM_Central` | `ws_id` | `{"order_id", "operator_id", "ws_id", "duracion", "origen"}` |
| `wm-central` | `WM_Central` | `WM_WS_E`, `WM_FO` | `ws_id` / `'CENTRAL'` | Según campo `tipo`: `ORDEN` (`REGAR`, `PARAR`, `BLOQUEAR`, `ACTIVAR`), `NOTIFICACION` (pasos y resumen), `ESTADO_RED` (foto global y últimos riegos). |
| `wm-eventos-ws` | `WM_WS_E` | `WM_Central` | `ws_id` | `{"ws_id", "evento": ("ENGINE_LISTO"\|"RIEGO_INICIADO"\|"RIEGO_RECHAZADO"\|"RIEGO_FINALIZADO"), "order_id", "motivo", "segundos", "volumen"}` |
| `wm-telemetria` | `WM_WS_E` | `WM_Central`, `WM_FO` | `ws_id` | `{"ws_id", "order_id", "caudal", "volumen", "segundos"}` (publicado a 1 Hz). |

---

## 4. Descripción Detallada de los Componentes Desarrollados

### 4.1 Módulo Central: `WM_Central`
El módulo central representa el cerebro del sistema y reside en la nube. Coordina el censo de estaciones, el ciclo de vida de los riegos y el cuadro de mandos interactivo.

- **Concurrencia Multihilo:**
  - `bucle_sockets`: Servidor concurrente que acepta conexiones entrantes de Monitores y genera un hilo de servicio (`atender_monitor`) por cada estación conectada.
  - `bucle_kafka`: Consumidor multihilo suscrito a peticiones de operarios, eventos de estación y telemetría de campo.
  - `bucle_estado_red`: Hilo periódico que difunde cada 2 segundos a través de Kafka el estado integral del ecosistema y los últimos riegos para los operarios.
  - `iniciar_panel`: Servidor HTTP multihilo (`ThreadingHTTPServer`) que sirve el panel web de control en el puerto 8080.
- **Máquina de Estados de la Estación:**
  Para evitar inconsistencias provocadas por transiciones no deterministas, el estado visible de una estación no se almacena como una variable estática arbitraria, sino que **se deduce funcionalmente** a partir de hechos objetivos verificables en memoria protegidos con `self.lock`:
  ```python
  @property
  def estado(self):
      if not self.conectada:
          return E.DESCONECTADA        # GRIS
      if self.salud == E.SALUD_KO:
          return E.FUGA                # ROJO
      if self.bloqueada:
          return E.FUERA_SERVICIO      # NARANJA (Out of Service)
      if self.riego:
          return E.REGANDO             # VERDE PARPADEANTE
      return E.DISPONIBLE              # VERDE
  ```
- **Lógica de Solicitudes y Gobernanza (Puntos 3 a 6 de la Mecánica):**
  1. Recepción de solicitud: Emisión inmediata de notificación informativa al operario (`"Solicitud recibida por CENTRAL"`).
  2. Validación estricta: Comprobación de existencia del operario en SQLite, existencia de la estación y disponibilidad (`DISPONIBLE`). Si no procede, emisión de `DENEGADA`.
  3. Reserva de estación y apertura: Envío de orden `REGAR` al Engine por Kafka y armado de temporizador de seguridad de 5 segundos (`TIMEOUT_AUTORIZACION`).
  4. Confirmación: Al recibir `RIEGO_INICIADO` del Engine, CENTRAL notifica `AUTORIZADA` al operario.
- **Órdenes Arbitrarias de CENTRAL (Punto 11 de la Mecánica):**
  A través del panel web o de su API, CENTRAL puede emitir de forma asíncrona hacia una estación específica o hacia todas (`*`):
  - **Iniciar Riego:** Actúa emulando la petición de un operario centralizado.
  - **Bloquear:** Marca la estación como bloqueada en memoria y SQLite, detiene cualquier riego activo y conmuta la estación a `FUERA_SERVICIO` (**Out of Service** en color naranja).
  - **Activar:** Retira el bloqueo, actualiza SQLite y devuelve la estación al estado `DISPONIBLE` (verde).

---

### 4.2 Capa de Persistencia: `database.py` (SQLite)
Implementa la capa relacional exigida mediante SQLite con aislamiento transaccional:
- **Esquema de Tablas:**
  - `estaciones`: `id` (PK, TEXT), `ubicacion` (TEXT), `bloqueada` (INTEGER), `fecha_alta` (TEXT).
  - `operarios`: `id` (PK, TEXT), `nombre` (TEXT).
  - `riegos`: `id` (PK AUTOINCREMENT), `order_id` (TEXT), `ws_id` (TEXT), `operator_id` (TEXT), `fin` (TEXT), `segundos` (INTEGER), `volumen_l` (REAL), `motivo` (TEXT).
- **Semántica de Reinicio (Punto 1 de la Mecánica):**
  Al reiniciar CENTRAL, las estaciones censadas en la tabla `estaciones` se cargan en memoria con `conectada = False`. Por tanto, el panel las muestra inmediatamente en estado **DESCONECTADA (Gris)** hasta que el Monitor físico establece el enlace por socket y supera la autenticación.

---

### 4.3 Cuadro de Mandos Web: `panel_web.py` y `panel.html`
Desarrollado mediante tecnologías web nativas (HTML5, CSS3, JavaScript ES6) servido directamente por Python sin bibliotecas externas:
- **Visualización Normativa de Estaciones:**
  - **Available:** Fondo verde sólido (`#16a34a`).
  - **Watering:** Fondo verde con animación CSS intermitente (`@keyframes parpadeo`), mostrando en tiempo real el caudal (L/min), volumen acumulado (L), tiempo transcurrido / total y el identificador del operario responsable.
  - **Leak:** Fondo rojo intenso (`#dc2626`).
  - **Out of Service:** Fondo naranja (`#ea580c`) con la leyenda textual explícita `Out of Service`.
  - **Disconnected:** Fondo gris (`#64748b`).
- **Interactividad en Tiempo Real:** Polling asíncrono cada 1.000 ms a `/api/estado`, actualización de tarjetas, tabla de operarios, auditoría de riegos y consola de trazas en directo.

---

### 4.4 Estación de Riego (Watering Station - WS)

La estación se descompone en dos procesos cooperantes e independientes:

#### A) Monitor de Estación (`WM_WS_M.py`)
- Actúa como enlace de control y vigilancia de la estación.
- **Cliente Sockets de CENTRAL:** Conecta al arranque, solicita autenticación (`AUTH#ws_id#ubicacion#salud`) y envía un latido periódico cada 2 segundos (`ESTADO`). Si CENTRAL está apagada, captura el error de red y muestra de forma controlada `Imposible conectar con WM_Central`, reintentando cada 3 segundos.
- **Servidor Sockets del Engine:** Escucha en un puerto local parametrizable. Al recibir al Engine, le suministra su `ws_id` y le consulta la salud a 1 Hz (`SALUD`). Si el Engine contesta `KO` o transcurren más de 3 segundos sin respuesta, conmuta inmediatamente la salud a `KO` y notifica a CENTRAL.

#### B) Motor Físico de Estación (`WM_WS_E.py`)
- Simula los actuadores y sensores hidráulicos.
- **Electroválvula y Caudalímetro:** Al autorizarse un riego, un hilo dedicado (`_regar`) abre la electroválvula simulada y muestrea un caudal nominal de $15.0 \pm 0.5\text{ L/min}$. Cada segundo integra el volumen suministrado y publica la telemetría a través de Kafka.
- **Menú Interactivo de Campo:**
  - `r <seg>`: Solicita a CENTRAL un riego manual en la propia estación. Si la estación está desvinculada del monitor (Averiada), rechaza la acción.
  - `p`: Cierra la electroválvula de inmediato por parada manual local.
  - `k`: Simula una avería catastrófica / fuga en caliente.
  - `o`: Notifica contingencia resuelta.
  - `e`: Consulta el estado operativo en pantalla.
- **Protección ante Desconexión del Monitor:** Si el socket con `WM_WS_M` se interrumpe, el Engine aborta el riego en curso, marca su estado como **AVERIADA** e invalida cualquier solicitud de riego manual hasta que el enlace sea restablecido.

---

### 4.5 Aplicación de Operarios: `WM_FO.py`
Proporciona la interfaz de los operarios de mantenimiento (jardineros):
- **Filtrado Selectivo de Eventos:** Se suscribe a `wm-central` y a `wm-telemetria`, pero descarta todo mensaje que no coincida con el `order_id` de la solicitud activa. De este modo, un operario **únicamente recibe las trazas y la telemetría de la WS que le está suministrando servicio**.
- **Observabilidad Global:** Recibe periódicamente los mensajes `ESTADO_RED` emitidos por CENTRAL, lo que le permite listar en cualquier momento todas las estaciones de la ciudad y su estado actual.
- **Modo Batch Automatizado (`servicios.txt`):** Capacidad de procesar un archivo secuencial de órdenes con formato `<WS_ID> <segundos>`. El módulo despacha la primera petición, aguarda su finalización completa (éxito o denegación), **espera estrictamente 4 segundos de pausa técnica** y procede con el siguiente servicio.
- **Recuperación tras Crash:** Si el operario se cierra o reinicia súbitamente, el riego en la estación física continúa inalterado; al volver a iniciar, el operario consulta la opción 2 del menú y observa el resultado del servicio en el histórico de últimos riegos persistido por CENTRAL.

---

## 5. Análisis de Resiliencia y Comportamiento ante Fallos

La resiliencia del sistema ha sido verificada sometiendo a los componentes a paradas abruptas (`Ctrl+C` y `docker stop / kill`):

### 5.1 Matriz de Estados de la Estación (Monitor y Engine)
Conforme a la tabla de estados requerida en la guía de evaluación:

| Estado Monitor (`WM_WS_M`) | Estado Engine (`WM_WS_E`) | Mensajes a Central | Estado Visible en CENTRAL |
|---|---|---|---|
| **Conectado (OK)** | **Conectado (OK)** | Recibe latidos periódicos `OK` | **Available (Verde)** |
| **Conectado (OK)** | **Caído / KO** | Recibe latidos con salud `KO` | **Leak / Averiada (Rojo)** |
| **Caído (KO)** | **Conectado (OK)** | No recibe latidos (timeout socket) | **Disconnected (Gris)** |
| **Caído (KO)** | **Caído (KO)** | No recibe latidos (timeout socket) | **Disconnected (Gris)** |

### 5.2 Escenarios Específicos Evaluados

```
                             FALLO EN COMPONENTE
                                      │
         ┌────────────────────────────┼────────────────────────────┐
         ▼                            ▼                            ▼
   Monitor Caído                Engine Caído                  Central Caída
         │                            │                            │
 CENTRAL detecta timeout       Monitor detecta KO         WS finaliza riego actual
   (<=6s) -> DESCONECTADA       (3s) -> Notifica FUGA       sin abortar volumen.
         │                            │                            │
 Engine pasa a AVERIADA       CENTRAL marca ROJO,         Reintentos automáticos
  y deniega riegos manuales.   guarda litros en BD y       con mensaje "Imposible
                               envía resumen a FO.         conectar con WM_Central".
```

1. **Caída Súbita del Monitor (`WM_WS_M`):**
   - *Comportamiento de Central:* Al vencer el timeout de 6 segundos sin latidos, CENTRAL marca la estación como `DESCONECTADA` (Gris).
   - *Comportamiento de la Estación:* El Engine detecta la desconexión del socket, finaliza cualquier riego en marcha (salvo que el Engine también hubiera caído), muestra el mensaje `AVERIADA` e invalida cualquier solicitud de riego manual.
2. **Caída Súbita del Engine (`WM_WS_E`):**
   - *Comportamiento:* A los 3 segundos de inactividad, el Monitor conmuta la salud a `KO` y remite la trama de avería a CENTRAL. CENTRAL marca inmediatamente la WS en **ROJO (Leak)** y despacha al operario afectado el resumen con el volumen de agua suministrado hasta el instante exacto del fallo.
   - *Garantía de No Pérdida:* Los litros acumulados en la última telemetría quedan asentados en SQLite. Al reiniciar el Engine, este vuelve a negociar su estado con el Monitor, el Monitor reporta `OK` y la estación regresa a **Available (Verde)** sin intervención humana.
3. **Caída del Operario (`WM_FO`) durante un Riego:**
   - *Comportamiento:* El Engine y CENTRAL continúan con el suministro programado sin interrupciones. Al completarse, el resumen final se archiva en la BD. Cuando el operario se vuelve a ejecutar, visualiza el resultado de su servicio en el histórico de riegos.
4. **Caída de Central (`WM_Central`):**
   - *Comportamiento:* Los Engines que se encuentren suministrando agua continúan regando de forma autónoma hasta expirar el tiempo prefijado. Una vez finalizado, se detienen de forma segura. Mientras CENTRAL permanezca fuera de servicio, ningún módulo puede admitir nuevas peticiones.
   - *Mensajes Controlados:* Tanto los operarios como los monitores capturan la pérdida de conexión y presentan en consola el mensaje normativo: `Imposible conectar con WM_Central`.

---

## 6. Guía de Despliegue y Puesta en Marcha Paso a Paso

Para la corrección de la práctica en los laboratorios se sigue de forma exacta la siguiente guía.

### 6.1 Fase 1: Despliegue Cloud en AWS EC2 (Learner Lab)
*Este entorno llega ya desplegado y operativo a la sesión de corrección.*

1. **Lanzamiento de Instancia en AWS Learner Lab:**
   - Instancia EC2: Ubuntu 24.04 LTS, tipo `t3.medium`.
   - **Reglas de Entrada del Security Group (TCP 0.0.0.0/0):**
     - Puerto `22`: Acceso SSH y despliegue CI/CD.
     - Puerto `5000`: Servidor de sockets de `WM_Central`.
     - Puerto `8080`: Panel de control web HTTP.
     - Puerto `9092`: Listener externo de Apache Kafka.
2. **Despliegue de los Contenedores Cloud:**
   ```bash
   git clone https://github.com/amf162-ua/SD-Practica1.git
   cd SD-Practica1/deploy/cloud
   cp .env.example .env
   # Configurar en .env la IP pública de la instancia EC2:
   # KAFKA_PUBLIC_HOST=54.x.y.z
   docker compose up -d --build
   ```
3. **Verificación de Salud:**
   ```bash
   docker compose ps
   # Salida esperada:
   # broker       Up (healthy)
   # wm-central   Up
   ```
   Abrir en el navegador: `http://<IP_PUBLICA_EC2>:8080` para verificar la carga del cuadro de mandos con las estaciones iniciales en gris (Desconectadas).

---

### 6.2 Fase 2: Despliegue en PC LAB 2 — Estaciones de Riego (`WM_WS`)
En el equipo asignado a las estaciones:

1. **Configuración de Variables:**
   ```bash
   cd SD-Practica1/deploy/lab
   cp .env.example .env
   # Configurar la IP pública de la EC2 en:
   # CENTRAL_HOST=54.x.y.z
   # KAFKA_HOST=54.x.y.z
   ```
2. **Lanzamiento de Estaciones:**
   ```bash
   docker compose -f compose.ws.yml up -d --build
   ```
   *Efecto inmediato:* `WS_01` y `WS_02` se autentican ante CENTRAL y en el panel web conmutan automáticamente de **Desconectada (Gris)** a **Available (Verde)**.
3. **Monitorización de Terminales:**
   - Para observar las trazas del Monitor:
     ```bash
     docker logs -f ws1-monitor
     ```
   - Para interactuar con el menú del Engine (riego manual, fuga):
     ```bash
     docker attach ws1-engine
     # Pulsar Enter para ver menú. Salir sin detener: Ctrl+P seguido de Ctrl+Q.
     ```

---

### 6.3 Fase 3: Despliegue en PC LAB 1 — Operarios de Campo (`WM_FO`)
En el equipo asignado a los operarios:

1. **Configuración de Variables:**
   ```bash
   cd SD-Practica1/deploy/lab
   # Asegurar que KAFKA_HOST apunta a la IP de la EC2 en .env
   ```
2. **Lanzamiento de Operarios:**
   ```bash
   docker compose -f compose.fo.yml up -d --build
   ```
3. **Demostración Autónoma Inicial (Sin Intervención):**
   - El operario `fo2` (`OP_02`) inicia de inmediato la ejecución automatizada de `servicios.txt`.
   - Observar cómo se suceden las peticiones sobre `WS_01`, `WS_02`, el riego con telemetría segundo a segundo, la denegación académica de `WS_99` y la pausa exacta de 4 segundos entre servicios:
     ```bash
     docker logs -f fo2
     ```
4. **Operación Interactiva de Campo:**
   - Conectarse al operario `fo1` (`OP_01`):
     ```bash
     docker attach fo1
     # Opción 1: Solicitar riego manual (ej. WS_01, 15 s)
     # Opción 2: Consultar estado de red y últimos riegos
     ```

---

## 7. Batería de Pruebas de Validación y Evidencias de Ejecución

Para la evaluación se reproduce la siguiente batería secuencial de pruebas:

| Id | Categoría | Acción Ejecutada | Resultado Esperado y Observado | Estado de Rúbrica |
|---|---|---|---|---|
| **P1** | **Arranque Autónomo** | Inicio de contenedores sin intervención. | `fo2` ejecuta `servicios.txt`; telemetría fluye en pantalla; panel muestra verde parpadeante en la WS regando y registra el resumen en SQLite. | **CUMPLE** |
| **P2** | **Censo SQLite Inicial** | Reinicio de CENTRAL con `docker restart wm-central`. | Las estaciones preexistentes en SQLite se muestran en Gris (`DESCONECTADA`) hasta que los monitores reconectan solos y pasan a Verde. | **CUMPLE** |
| **P3** | **Rechazo WS No Registrada** | Conexión de un Monitor con ID desconocido (`WS_99`). | CENTRAL deniega la autenticación: `KO#WS_NO_REGISTRADA`. Monitor informa que debe darse de alta previamente. | **CUMPLE** |
| **P4** | **Alta en Caliente** | Registro de `WS_03` desde el formulario web de CENTRAL. | La estación pasa a figurar en SQLite; se conecta su contenedor y se autentica exitosamente pasando a Verde. | **CUMPLE** |
| **P5** | **Riego desde Operario** | `fo1` solicita riego de 20s en `WS_01`. | Notificaciones paso a paso (`INFO` $\to$ `AUTORIZADA`); telemetría en terminal de `fo1`; tarjeta en panel parpadea con caudal, volumen y operario. | **CUMPLE** |
| **P6** | **Exclusión de Riego Simultáneo** | Con `WS_01` regando, `fo1` pide regar en `WS_01`. | CENTRAL deniega la orden: `DENEGADA: La estación no está disponible (estado: REGANDO)`. | **CUMPLE** |
| **P7** | **Simulación de Fuga** | Durante un riego en `WS_01`, pulsar `k` en el Engine. | Engine cierra electroválvula (`FIN_FUGA`); Monitor informa avería; Central conmuta la estación a **ROJO (Leak)** y envía resumen con los litros regados al FO. | **CUMPLE** |
| **P8** | **Resolución de Fuga** | Pulsar `o` en el Engine averiado. | Monitor reporta salud `OK`; CENTRAL restablece la WS a **VERDE (Available)**. | **CUMPLE** |
| **P9** | **Órdenes de Central (Pto. 11)** | Pulsar *Bloquear* sobre todas las estaciones en el panel. | Todos los riegos se abortan; tarjetas pasan a **NARANJA ("Out of Service")**; nuevos riegos quedan bloqueados. | **CUMPLE** |
| **P10** | **Reactivación Central** | Pulsar *Activar* sobre todas las estaciones. | Tarjetas retornan a **VERDE (Available)** de forma simultánea. | **CUMPLE** |
| **P11** | **Resiliencia Monitor KO** | `docker stop ws1-monitor`. | CENTRAL detecta ausencia de latido en $\le 6\text{ s}$ y pasa a Gris (`DESCONECTADA`); el Engine pasa a `AVERIADA` y deniega riegos locales. | **CUMPLE** |
| **P12** | **Resiliencia Engine KO** | `docker stop ws1-engine`. | Monitor detecta inactividad en 3s y notifica avería; Central pasa a Rojo (`FUGA`); ningún dato de riego previo se pierde en la BD. | **CUMPLE** |
| **P13** | **Resiliencia Operario KO** | `docker stop fo1` durante un riego. | El riego en la estación física sigue su curso completo; al reiniciar `fo1`, el operario consulta la opción 2 y observa el resultado en el histórico. | **CUMPLE** |
| **P14** | **Reintento Broker Caído** | Parada temporal del broker Kafka (`docker stop broker`). | Los módulos no crashean: entran en bucle de reconexión controlada cada 3s y recuperan la sesión en cuanto el broker vuelve. | **CUMPLE** |

---

## 8. Cumplimiento Exhaustivo de la Guía de Corrección

A continuación se detalla la autoevaluación formal contrastando cada requerimiento de la Guía de Corrección:

| Apartado de Evaluación | Valoración Máxima | Cumplimiento del Proyecto | Justificación Técnica |
|---|:---:|:---:|---|
| **1. Despliegue, modularidad y escalabilidad** | **2.0 ptos** | **CUMPLE (E)** | - Parte Cloud (Central + Kafka + SQLite) desplegada en AWS EC2 con Docker Compose.<br>- Módulos de laboratorio (FO y WS) contenerizados y desplegables sin entornos de compilación ni IDEs.<br>- Estructura de repositorio idéntica a la especificada (`WM_Central`, `WM_WS`, `WM_FO`, `prompts`, `README.md`, `Nombres.txt`).<br>- Escalabilidad demostrada: despliegue de múltiples instancias mediante compose o comandos CLI directos. |
| **2. Funcionamiento base** | **3.0 ptos** | **CUMPLE (E)** | - Parametrización íntegra por línea de comandos y variables de entorno; sin constantes fijas ni necesidad de recompilar.<br>- Demostración autónoma inicial sin interacción humana mediante `fo2` y `servicios.txt`.<br>- SQLite carga estaciones preexistentes en estado inicial Desconectada (Gris).<br>- Panel web de CENTRAL con paleta y leyendas normativas: Available (Verde), Watering (Verde parpadeante con telemetría e ID de operario), Leak (Rojo), Out of Service (Naranja con texto explícito "Out of Service") y Disconnected (Gris).<br>- Autenticación estricta con denegación de WS no registradas (`KO#WS_NO_REGISTRADA`).<br>- Control de riego operativo desde los 3 puntos: Operario (FO), Central y Estación (Engine).<br>- Simulación de fallo ('k') y recuperación ('o') en tiempo de ejecución.<br>- Operarios aislados: solo reciben telemetría de su servicio pero observan el estado de la red. |
| **3. Resiliencia y tolerancia a fallos** | **3.0 ptos** | **CUMPLE (E)** | - Aislamiento total de componentes: la caída de uno no degrada a los no vinculados.<br>- Caída de Monitor: WS pasa a Desconectada en Central en $\le 6\text{s}$, Engine pasa a Averiada y no admite riegos manuales.<br>- Caída de Engine: Monitor notifica avería, Central marca Fuga (Rojo) y salva en SQLite el volumen suministrado hasta el fallo.<br>- Caída de FO: Riego físico no se detiene; resultado visible al recuperarse.<br>- Caída de Central: Riego en curso culmina normalmente; captura de error controlado `"Imposible conectar con WM_Central"`.<br>- Tabla de verdad Monitor/Engine cumplida rigurosamente. |
| **4. Aspectos Generales y Técnicos Reseñables** | **2.0 ptos** | **CUMPLE (E)** | - Memoria técnica completa, rigurosa y ordenada con portada, arquitectura y guía paso a paso.<br>- Protocolo propietario de sockets con tramas `<STX><DATA><ETX><LRC>`, control `ENQ`/`ACK` y verificación LRC mediante XOR byte a byte.<br>- Automatización CI/CD con GitHub Actions hacia AWS EC2.<br>- Registro transparente y detallado de los prompts de Inteligencia Artificial en `prompts/prompts.txt`. |
| **TOTAL** | **10.0 / 10.0** | **APTO MÁXIMO** | Todos los requisitos funcionales, de diseño y de resiliencia satisfechos al 100%. |

---

## 9. Aspectos Técnicos Reseñables y Mejoras Implementadas

1. **Protocolo de Sockets con Detección de Errores e Integridad LRC:**
   Se implementó la especificación opcional recomendada en la página 12 del enunciado, dotando a la comunicación de bajo nivel de tramas robustas con delimitadores binarios `<STX>` (0x02) y `<ETX>` (0x03), verificación de suma longitudinal XOR byte a byte (`LRC`), handshakes `ENQ`/`ACK` y soporte para reenvío automático ante tramas corruptas (`NACK`).
2. **Buffer de Reensamblado TCP (`CanalTramas`):**
   Se diseñó una clase de gestión de sockets que resuelve el problema del *TCP stream segmentation*, permitiendo extraer tramas completas sin asumir que una llamada al sistema `recv()` equivale a un único mensaje de aplicación.
3. **Persistencia Transaccional Thread-Safe en SQLite:**
   El módulo `BaseDatos` opera mediante cerrojos reentrantes que permiten a múltiples hilos de servidor y consumidores de Kafka interactuar con SQLite de forma completamente segura, evitando bloqueos de tipo `database is locked` y garantizando auditoría histórica persistente de todos los riegos realizados.
4. **Pipeline Automatizado de CI/CD (GitHub Actions $\to$ AWS EC2):**
   Se configuró un flujo de integración y entrega continua en `.github/workflows/deploy-aws.yml`. Cada confirmación (*push*) en la rama principal valida la compilación sintáctica de todo el código Python y se conecta mediante SSH seguro a la máquina virtual en AWS para actualizar y relanzar los contenedores de forma desatendida.
5. **Panel de Control Reactivo sin Dependencias Externas:**
   El panel web fue construido utilizando HTML5 semántico, CSS con variables de diseño moderno y animaciones nativas por GPU, y Vanilla JS para el refresco asíncrono, garantizando que el sistema sea autosuficiente y no dependa de redes de distribución de contenido (CDNs) ni frameworks pesados para su visualización.

---

## 10. Uso Responsable de Herramientas de Inteligencia Artificial

De conformidad con las directrices académicas de la Universidad de Alicante sobre el uso responsable de la Inteligencia Artificial:
- **Transparencia y Trazabilidad:** Se ha documentado el catálogo exhaustivo de directrices y prompts de desarrollo en el fichero [`prompts/prompts.txt`](prompts/prompts.txt), el cual forma parte integral del repositorio oficial.
- **Rol de la IA:** Las herramientas de IA fueron utilizadas como acelerador para la generación de esqueletos iniciales de código, optimización de hojas de estilo CSS y diseño de casos límite para pruebas de resiliencia.
- **Comprensión y Dominio Técnico:** Todo el código fuente resultante ha sido minuciosamente revisado, comprendido y validado por los integrantes del equipo. Los estudiantes están plenamente capacitados para explicar la arquitectura, el flujo de hilos, los mecanismos de sincronización, la estructura de las tramas de sockets y las decisiones de diseño durante la evaluación presencial en el laboratorio.

---

### Anexo: Estructura del Código en el Repositorio

```text
SD-Practica1/
├── common/                  # Código compartido por todos los módulos
│   ├── protocolo.py         # Protocolo de tramas <STX><DATA><ETX><LRC> y CanalTramas
│   ├── kafka_utils.py       # Tópicos, utilidades de productor/consumidor resiliente
│   └── estados.py           # Constantes de estados y motivos de finalización
├── WM_Central/              # Núcleo del sistema (despliegue en AWS Cloud)
│   ├── WM_Central.py        # Servidor concurrente de sockets, Kafka y orquestación
│   ├── database.py          # Capa de datos SQLite (estaciones, operarios, riegos)
│   ├── panel_web.py         # Servidor HTTP multihilo para cuadro de mandos
│   ├── panel.html           # Interfaz web interactiva en tiempo real
│   └── Dockerfile           # Imagen Docker para el Core System
├── WM_WS/                   # Estación de riego (PC LAB 2)
│   ├── WM_WS_M.py           # Monitor: Cliente socket Central y servidor Engine
│   ├── WM_WS_E.py           # Engine: Actuador electroválvula, caudalímetro y menú
│   └── Dockerfile           # Imagen Docker para componentes de la estación
├── WM_FO/                   # Operario de campo (PC LAB 1)
│   ├── WM_FO.py             # Aplicación de operario: interactiva y batch
│   ├── servicios.txt        # Fichero por lotes de demostración de servicios
│   └── Dockerfile           # Imagen Docker para operarios
├── deploy/                  # Manifiestos de despliegue con Docker Compose
│   ├── cloud/compose.yml    # Kafka KRaft + WM_Central + SQLite (AWS EC2)
│   └── lab/
│       ├── compose.ws.yml   # WS_01 y WS_02 (PC LAB 2)
│       └── compose.fo.yml   # fo1 y fo2 (PC LAB 1)
├── .github/workflows/       # Automatización CI/CD
│   └── deploy-aws.yml       # Workflow GitHub -> AWS EC2 vía SSH
├── prompts/
│   └── prompts.txt          # Registro histórico y detallado de prompts de IA
├── MEMORIA.md               # Memoria técnica oficial de la práctica
├── GUIA.md                  # Guía de funcionamiento y pruebas paso a paso
├── README.md                # Documentación del repositorio y resumen de uso
├── Nombres.txt              # Identificación formal de los alumnos y DNI
└── requirements.txt         # Dependencias Python (kafka-python)
```
