# Water Management Network · Sistemas Distribuidos 26/27

Simulación de una red de estaciones de riego gestionada desde una central. Usa **sockets TCP**
con protocolo propio `<STX><DATA><ETX><LRC>`, **Kafka** para el streaming de eventos, **SQLite**
para la persistencia y **Docker** para desplegar todos los módulos.

Despliegue: **AWS (EC2 del Learner Lab)** para CENTRAL + BD + Kafka, y PCs del laboratorio para
las estaciones (WS) y los operarios (FO). Se usa AWS porque Railway no admite registros nuevos.

> 📘 **[GUIA.md](GUIA.md)**: explicación del código, puesta en marcha en un solo PC y batería de pruebas completa.

---

## Estructura

```text
SD-Practica1/
├── common/                  Código compartido por todos los módulos
│   ├── protocolo.py         Tramas <STX><DATA><ETX><LRC>, ACK/NACK, ENQ/EOT
│   ├── kafka_utils.py       Tópicos y creación de productores/consumidores
│   └── estados.py           Estados de una WS y motivos de fin de riego
├── WM_Central/              CENTRAL (cloud)
│   ├── WM_Central.py        Lógica: sockets con los Monitores + Kafka + órdenes del punto 11
│   ├── database.py          SQLite: estaciones, operarios e histórico de riegos
│   ├── panel_web.py         Servidor HTTP del panel de monitorización
│   ├── panel.html           Panel (estaciones con colores, órdenes, altas, registro)
│   └── Dockerfile
├── WM_WS/                   Estación de riego (PC LAB 2)
│   ├── WM_WS_M.py           Monitor: autenticación en CENTRAL y salud del Engine
│   ├── WM_WS_E.py           Engine: electroválvula, caudalímetro y menú de la estación
│   └── Dockerfile
├── WM_FO/                   Operario de campo (PC LAB 1)
│   ├── WM_FO.py
│   ├── servicios.txt        Fichero de servicios de ejemplo
│   └── Dockerfile
├── deploy/
│   ├── cloud/compose.yml    Kafka + CENTRAL (EC2)
│   └── lab/                 compose.ws.yml (estaciones) y compose.fo.yml (operarios)
├── .github/workflows/       CI/CD GitHub -> AWS
├── requirements.txt         kafka-python
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
| WS_M ↔ CENTRAL | Sockets | Autenticación de la WS y su estado de salud (latido cada 2 s) |
| WS_E ↔ WS_M | Sockets | El Monitor asigna el ID al Engine y comprueba su salud cada segundo |
| FO / WS_E ↔ CENTRAL | Kafka | Solicitudes, órdenes, eventos de riego, telemetría y notificaciones |

### Protocolo de sockets

Cada mensaje viaja como `<STX>CODIGO#campo1#...<ETX><LRC>` (LRC = XOR de los bytes de DATA). Quien
recibe responde `ACK` si el LRC es correcto o `NACK` para que se reenvíe (hasta 3 veces). La conexión
empieza con `ENQ`/`ACK` y termina con `EOT`.

| Origen → destino | Petición | Respuesta |
|---|---|---|
| Monitor → CENTRAL | `AUTH#<ws_id>#<ubicación>#<OK\|KO>` | `OK#<ws_id>` o `KO#ID_YA_CONECTADO` |
| Monitor → CENTRAL | `ESTADO#<ws_id>#<OK\|KO>` (al cambiar y cada 2 s) | `OK` |
| Engine → Monitor | `HOLA` | `ID#<ws_id>` |
| Monitor → Engine | `SALUD` (cada segundo) | `OK` o `KO` |

- Si CENTRAL no recibe nada de un Monitor en 6 s, la WS pasa a **DESCONECTADA**.
- Si el Engine responde `KO` o no responde en 3 s, el Monitor informa `KO` → **FUGA**.

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

### Flujo de una solicitud de riego

1. El FO publica la solicitud. CENTRAL avisa *"Solicitud recibida"*.
2. CENTRAL comprueba que el operario existe en la BD y que la WS está **DISPONIBLE**; si no, *DENEGADA*.
3. CENTRAL reserva la WS y envía `REGAR` al Engine (*"Solicitando apertura..."*).
4. El Engine abre la electroválvula y publica `RIEGO_INICIADO` → CENTRAL notifica *AUTORIZADA*.
   Si el Engine no contesta en 5 s, la solicitud se deniega.
5. Cada segundo el Engine publica la telemetría (panel de CENTRAL y FO la muestran).
6. Al terminar (tiempo cumplido, parada manual, fuga, bloqueo o desconexión) el Engine publica
   `RIEGO_FINALIZADO`, CENTRAL lo guarda en la BD y envía el **resumen** al operario.

### Estados de una WS (panel de CENTRAL)

| Estado | Color | Cuándo |
|---|---|---|
| Disponible | Verde | Conectada, sana, no bloqueada y sin riego |
| Regando | Verde parpadeando | Riego en curso (caudal, volumen, tiempo y operario) |
| Fuga | Rojo | El Engine responde KO o no responde |
| Fuera de Servicio | Naranja | Bloqueada por CENTRAL (persistente en la BD) |
| Desconectada | Gris | El Monitor no está conectado a CENTRAL |

---

## Uso de cada módulo

```bash
python WM_Central/WM_Central.py <puerto_sockets> <ip_broker> [<puerto_broker>] [--panel 8080] [--db ruta]
python WM_WS/WM_WS_M.py <puerto_engine> <ip_central> <puerto_central> <ws_id> [<ubicación>]
python WM_WS/WM_WS_E.py <ip_broker> <puerto_broker> <ip_monitor> <puerto_monitor>
python WM_FO/WM_FO.py <ip_broker> <puerto_broker> <id_operador> [<fichero_servicios>]
```

**Menú del Engine:** `r <seg>` riego manual · `p` parar riego · `k` simular fuga (KO) · `o` fuga resuelta · `e` estado · Enter muestra el menú.

**Menú del operario:** solicitar riego, ver estaciones, ejecutar un fichero de servicios
(formato `<WS_ID> <segundos>` por línea; 4 s de pausa entre servicios).

**Panel de CENTRAL** (`http://<ip>:8080`): estaciones en tiempo real, órdenes a una o a todas las WS
(iniciar riego, bloquear, activar), alta de estaciones y operarios, últimos riegos y registro de eventos.
Operarios dados de alta por defecto: `OP_01` … `OP_04`. Una WS desconocida se da de alta sola al autenticarse.

---

## Guía de despliegue

### 1. Parte Cloud en AWS Learner Lab

1. *Start Lab* y abrir la consola de AWS. Lanzar una instancia **EC2**:
   Ubuntu 24.04, tipo `t3.medium` (Kafka necesita ~1 GB de RAM), par de claves `vockey`.
2. En el **Security Group** de la instancia, abrir de entrada (TCP, origen `0.0.0.0/0`):
   `22` (SSH), `5000` (sockets CENTRAL), `8080` (panel), `9092` (Kafka).
3. Conectarse por SSH e instalar Docker:
   ```bash
   sudo apt update && sudo apt install -y docker.io docker-compose-v2 git
   sudo usermod -aG docker $USER && newgrp docker
   ```
4. Clonar el repositorio y configurar la IP pública:
   ```bash
   git clone https://github.com/amf162-ua/SD-Practica1.git && cd SD-Practica1/deploy/cloud
   cp .env.example .env && nano .env        # KAFKA_PUBLIC_HOST=<IP pública de la EC2>
   docker compose up -d --build
   docker compose ps                        # broker (healthy) y wm-central (Up)
   ```
5. Comprobar el panel en `http://<IP pública>:8080`. Logs de CENTRAL: `docker logs -f wm-central`.

> La IP pública de la EC2 cambia cada vez que se reinicia el Learner Lab: actualizar `.env`
> (aquí y en los PCs del laboratorio) y repetir `docker compose up -d`.

### 2. PC LAB 2 — Estaciones de riego

```bash
cd deploy/lab
cp .env.example .env                       # CENTRAL_HOST y KAFKA_HOST = IP pública de la EC2
docker compose -f compose.ws.yml up -d --build
docker logs -f ws1-monitor                 # pantalla del Monitor de WS_01
docker attach ws1-engine                   # menú del Engine de WS_01 (Ctrl+P Ctrl+Q para salir)
```

### 3. PC LAB 1 — Operarios

```bash
cd deploy/lab
cp .env.example .env                       # KAFKA_HOST = IP pública de la EC2
docker compose -f compose.fo.yml up -d --build
docker attach fo1                          # operario OP_01 (menú)
docker attach fo2                          # operario OP_02 (ejecuta servicios.txt y luego menú)
```

### 4. Todo en un solo PC (desarrollo)

Igual que los pasos anteriores, pero poniendo en los `.env` la IP del PC en la red local
(`ipconfig`), no `localhost`: los contenedores del laboratorio tienen que poder llegar a Kafka.

### 5. CI/CD: VS Code → GitHub → AWS

El workflow [.github/workflows/deploy-aws.yml](.github/workflows/deploy-aws.yml) se ejecuta en cada
push a `main`: compila el código y, por SSH, hace `git pull` y `docker compose up -d --build` en la EC2.
Secrets del repositorio: `EC2_HOST` (IP pública actual), `EC2_USER` (`ubuntu`) y `EC2_SSH_KEY`
(contenido de `labsuser.pem`). El repositorio debe estar clonado en `~/SD-Practica1` en la EC2.

---

## Pruebas de resiliencia para la corrección

| Prueba | Qué hacer | Resultado esperado |
|---|---|---|
| Reinicio de CENTRAL | `docker restart wm-central` | Las WS aparecen DESCONECTADAS y se reconectan solas |
| Fuga en riego | `k` en el Engine durante un riego | Riego abortado, WS en ROJO, resumen al operario |
| Engine caído | `docker stop ws1-engine` | El Monitor informa avería → ROJO |
| Monitor caído | `docker stop ws1-monitor` | GRIS en ≤ 6 s; el riego en curso se cierra |
| Bloqueo | *Bloquear* en el panel durante un riego | Riego parado, NARANJA "Fuera de Servicio" |
| WS ocupada | Pedir riego en una WS que ya riega | DENEGADA |
| Operario no registrado | `WM_FO.py ... OP_99` | DENEGADA |
