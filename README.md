# Water Management Network · Sistemas Distribuidos 26/27

Simulación de una red de estaciones de riego gestionada desde una central. Usa **sockets TCP**
con protocolo propio `<STX><DATA><ETX><LRC>`, **Kafka** para el streaming de eventos, **SQLite**
para la persistencia y **Docker** para desplegar todos los módulos.

Despliegue: **AWS (EC2 del Learner Lab)** para CENTRAL + BD + Kafka, y PCs del laboratorio para
las estaciones (WS) y los operarios (FO). Se usa AWS porque Railway no admite registros nuevos.

> 📘 **[GUIA.md](GUIA.md)**: cómo funciona el sistema, por qué está hecho así y qué pasa si cae cada pieza.
> 🚀 **[DESPLIEGUE.md](DESPLIEGUE.md)**: pasos del despliegue y guion de la demostración para la revisión.

---

## Estructura

```text
SD-Practica1/
├── common/                  Código compartido por todos los módulos
│   ├── protocolo.py         Tramas <STX><DATA><ETX><LRC>, ACK/NACK, ENQ/EOT
│   ├── kafka_utils.py       Tópicos, productor y consumidor con reconexión automática
│   └── estados.py           Estados de una WS y motivos de fin de riego
├── WM_Central/              CENTRAL (cloud)
│   ├── WM_Central.py        Sockets con los Monitores + Kafka + órdenes del punto 11
│   ├── database.py          SQLite: estaciones, operarios e histórico de riegos
│   ├── panel_web.py         Servidor HTTP del panel (API JSON)
│   ├── panel.html           Panel: estaciones con colores, órdenes, altas y registro
│   └── Dockerfile
├── WM_WS/                   Estación de riego (PC LAB 2)
│   ├── WM_WS_M.py           Monitor: alta/autenticación en CENTRAL y salud del Engine
│   ├── WM_WS_E.py           Engine: electroválvula, caudalímetro y menú de la estación
│   └── Dockerfile           Una imagen para Monitor y Engine
├── WM_FO/                   Operario de campo (PC LAB 1)
│   ├── WM_FO.py
│   ├── servicios.txt        Fichero de servicios de ejemplo
│   └── Dockerfile
├── deploy/
│   ├── cloud/compose.yml    Kafka + CENTRAL (EC2)
│   └── lab/                 compose.ws.yml (estaciones) y compose.fo.yml (operarios)
├── .github/workflows/deploy.yml   CI: construye la imagen de CENTRAL y la publica en Docker Hub
├── Dockerfile               Imagen de CENTRAL que construye el CI
├── requirements.txt         kafka-python
├── README.md · GUIA.md · DESPLIEGUE.md · MEMORIA.md · Nombres.txt
└── prompts/                 Prompts de IA utilizados
```

---

## Arquitectura

```text
                         ┌──────────── AWS EC2 (Docker) ────────────┐
   PC LAB 2              │                                           │          PC LAB 1
 ┌───────────┐  sockets  │  ┌────────────┐   ┌────────┐              │  Kafka  ┌────────┐
 │ WM_WS_M   │──────────────│ WM_Central │───│ SQLite │              │◄────────│ WM_FO  │
 │  ▲ sockets│   :5000   │  │  panel:8080│   └────────┘              │  :9092  └────────┘
 │  ▼        │           │  └─────┬──────┘                           │
 │ WM_WS_E   │───────────────────►│ Kafka (broker) :9092             │
 └───────────┘   Kafka   │        └──────────────────────────────────┘
```

| Enlace | Tecnología | Para qué |
|---|---|---|
| WS_M ↔ CENTRAL | Sockets | Alta y autenticación de la WS y su estado de salud (latido cada 2 s) |
| WS_E ↔ WS_M | Sockets | El Monitor asigna el ID al Engine y comprueba su salud cada segundo |
| FO / WS_E ↔ CENTRAL | Kafka | Solicitudes, órdenes, eventos de riego, telemetría y notificaciones |

### Protocolo de sockets

Cada mensaje viaja como `<STX>CODIGO#campo1#...<ETX><LRC>` (LRC = XOR de los bytes de DATA). Quien
recibe responde `ACK` si el LRC es correcto o `NACK` para que se reenvíe (hasta 3 veces). La conexión
empieza con `ENQ`/`ACK` y termina con `EOT`.

| Origen → destino | Petición | Respuesta |
|---|---|---|
| Monitor → CENTRAL | `REGISTRO#<ws_id>#<ubicación>` (solo con `--registrar`) | `OK#<ws_id>` o `OK#YA_REGISTRADA` |
| Monitor → CENTRAL | `AUTH#<ws_id>#<ubicación>#<OK\|KO>` | `OK#<ws_id>`, `KO#WS_NO_REGISTRADA` o `KO#ID_YA_CONECTADO` |
| Monitor → CENTRAL | `ESTADO#<ws_id>#<OK\|KO>` (al cambiar y cada 2 s) | `OK` |
| Engine → Monitor | `HOLA` | `ID#<ws_id>` |
| Monitor → Engine | `SALUD` (cada segundo) | `OK` o `KO` |

- Una WS **no registrada** en CENTRAL es rechazada al autenticarse (`KO#WS_NO_REGISTRADA`).
- Si CENTRAL no recibe nada de un Monitor en 6 s, la WS pasa a **Disconnected**.
- Si el Engine responde `KO` o no responde en 3 s, el Monitor informa `KO` → **Leak**.

### Tópicos de Kafka

Cuatro tópicos: lo que entra a CENTRAL, lo que sale de CENTRAL y lo que publican las estaciones,
separando los eventos puntuales de la telemetría continua.

| Tópico | Productor → consumidor | Contenido |
|---|---|---|
| `wm-solicitudes` | FO, WS_E (riego manual) → CENTRAL | Petición de riego |
| `wm-central` | CENTRAL → WS_E, FO | Según el campo `tipo`: `ORDEN` (`REGAR`, `PARAR`, `BLOQUEAR`, `ACTIVAR`) para los Engines, `NOTIFICACION` (cada paso de la solicitud y el resumen final) y `ESTADO_RED` (todas las WS cada 2 s) para los operarios |
| `wm-eventos-ws` | WS_E → CENTRAL | `ENGINE_LISTO`, `RIEGO_INICIADO`, `RIEGO_RECHAZADO`, `RIEGO_FINALIZADO` |
| `wm-telemetria` | WS_E → CENTRAL, FO | Caudal y volumen acumulado cada segundo |

Cada consumidor filtra lo que le interesa: el Engine por su `ws_id`, el operario por el
`order_id` de su solicitud, y ambos descartan los `tipo` que no son para ellos.
CENTRAL y los FO usan **grupos de consumidor fijos** (al volver de una caída reciben lo que se
perdieron); el Engine usa uno **nuevo** en cada arranque (no debe ejecutar órdenes antiguas).
Detalles en [GUIA.md](GUIA.md#4-kafka-tópicos-y-grupos).

### Flujo de una solicitud de riego

1. El FO publica la solicitud. CENTRAL avisa *"Solicitud recibida"*.
2. CENTRAL comprueba que el operario existe en la BD, que la WS existe y está **Available** y que no
   tiene otra petición en curso; si no, *DENEGADA* con el motivo.
3. CENTRAL reserva la WS y envía `REGAR` al Engine (*"Solicitando apertura..."*).
4. El Engine abre la electroválvula y publica `RIEGO_INICIADO` → CENTRAL notifica *AUTORIZADA*.
   Si el Engine no contesta en 5 s, la solicitud se deniega.
5. Cada segundo el Engine publica la telemetría (el panel de CENTRAL y el FO la muestran).
6. Al terminar (tiempo cumplido, parada manual, fuga, bloqueo, desconexión o avería) el Engine
   publica `RIEGO_FINALIZADO`, CENTRAL lo guarda en la BD (una sola vez por `order_id`) y envía el
   **resumen** al operario.

### Estados de una WS (panel de CENTRAL)

| Estado (panel) | Color | Cuándo |
|---|---|---|
| Available | Verde | Conectada, sana, no bloqueada y sin riego |
| Watering | Verde parpadeando | Riego en curso (caudal, volumen, tiempo y operario) |
| Leak | Rojo | El Engine responde KO (fuga) o no responde (avería) |
| Out of Service | Naranja | Bloqueada por CENTRAL (persistente en la BD) |
| Disconnected | Gris | El Monitor no está conectado a CENTRAL |

### Resiliencia

| Cae | Qué pasa | Al volver |
|---|---|---|
| Monitor | Disconnected en ≤ 6 s; el riego se cierra; el Engine queda AVERIADA y rechaza riegos | Se reconecta solo |
| Engine | Leak en ≤ 3 s; CENTRAL guarda el riego con la última telemetría y avisa al FO | Envía el estado final del riego (motivo AVERIA) |
| FO | El riego sigue | Muestra el resultado del servicio que se perdió |
| CENTRAL | Los riegos en curso terminan; el FO avisa y no envía peticiones | Carga la BD, registra los riegos terminados y descarta las peticiones antiguas |
| Kafka | Avisos controlados, sin errores | Todo sigue sin reiniciar ningún módulo |

Explicación en [GUIA.md](GUIA.md#7-qué-pasa-si-cae-cada-pieza) y pruebas en [DESPLIEGUE.md](DESPLIEGUE.md#52-resiliencia-caídas-abruptas).

---

## Uso de cada módulo

```bash
python WM_Central/WM_Central.py <puerto_sockets> <ip_broker> [<puerto_broker>] [--panel 8080] [--db ruta]
python WM_WS/WM_WS_M.py <puerto_engine> <ip_central> <puerto_central> <ws_id> [<ubicación>] [--registrar]
python WM_WS/WM_WS_E.py <ip_broker> <puerto_broker> <ip_monitor> <puerto_monitor>
python WM_FO/WM_FO.py <ip_broker> <puerto_broker> <id_operador> [<fichero_servicios>]
```

En el despliegue no se lanzan a mano: los composes de [deploy/](deploy/) los arrancan con estos
argumentos, tomando IPs y puertos del fichero `.env`.

**Menú del Engine:** `r <seg>` riego manual · `p` parar riego · `k` simular fuga (KO) · `o` fuga resuelta · `e` estado · Enter muestra el menú.

**Menú del operario:** `1` solicitar riego · `2` ver estaciones y últimos riegos · `3` ejecutar un fichero
de servicios (formato `<WS_ID> <segundos>` por línea; 4 s de pausa entre servicios) · `0` salir.

**Panel de CENTRAL** (`http://<ip>:8080`): estaciones en tiempo real, órdenes a una o a todas las WS
(iniciar riego, bloquear, activar), alta de estaciones y operarios, últimos riegos y registro de eventos.

**Datos iniciales** (al crear la BD vacía): operarios `OP_01` … `OP_04` y estaciones `WS_01`
(Parque de El Palmeral) y `WS_02` (Jardines de Canalejas). Una WS nueva se da de alta desde el panel
o arrancando su Monitor con `--registrar`; si no, CENTRAL la rechaza.

---

## Despliegue

El despliegue completo, paso a paso y con el guion de la demostración, está en
**[DESPLIEGUE.md](DESPLIEGUE.md)**. Resumen:

| Dónde | Qué | Cómo |
|---|---|---|
| EC2 | Kafka + CENTRAL + SQLite | `deploy/cloud`: `.env` con la IP pública y `docker compose up -d --build` |
| PC LAB 2 | WS_01 y WS_02 (Monitor + Engine) | `deploy/lab`: `.env` con la IP de la EC2 y `docker compose -f compose.ws.yml up -d` |
| PC LAB 1 | fo1 (menú) y fo2 (fichero de servicios) | `deploy/lab`: `docker compose -f compose.fo.yml up -d` |

### Preparar la EC2 (una sola vez)

1. Lanzar una instancia **EC2** con Ubuntu (recomendado ≥ 2 GB de RAM, por Kafka).
2. **Security Group**, reglas de entrada TCP desde `0.0.0.0/0`: `22` (SSH), `5000` (sockets de CENTRAL),
   `8080` (panel) y `9092` (Kafka).
3. Conectarse (EC2 Instance Connect, usuario `ubuntu`), instalar Docker y clonar el repositorio:
   ```bash
   sudo apt update && sudo apt install -y docker.io docker-compose-v2 git
   sudo usermod -aG docker $USER && newgrp docker
   git clone https://github.com/amf162-ua/SD-Practica1.git
   ```

> La IP pública de la EC2 cambia en cada sesión del Learner Lab: hay que actualizarla en el `.env`
> de la EC2 y en el de los PCs del laboratorio (ver [DESPLIEGUE.md](DESPLIEGUE.md)).

### CI/CD: VS Code → GitHub → AWS

Siguiendo el tutorial de AWS de la asignatura, el workflow
[.github/workflows/deploy.yml](.github/workflows/deploy.yml) se ejecuta en cada push a `main`:
construye la imagen de CENTRAL con el [Dockerfile](Dockerfile) de la raíz y la publica en Docker Hub
(`<DOCKER_USERNAME>/practica-aws:latest`, secrets `DOCKER_USERNAME` y `DOCKER_PASSWORD`).
Así cada cambio que se sube queda comprobado (la imagen se construye) y publicado.

Como en el tutorial, la actualización del servidor es manual: en la EC2, `git pull` y
`docker compose up -d --build` en `deploy/cloud`. CENTRAL se despliega en la EC2 junto a Kafka, y no en
ECS, porque necesita el puerto TCP 5000 para los sockets de los Monitores y una BD SQLite persistente.
