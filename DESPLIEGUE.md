# Despliegue — Día de la revisión

**Escenario:** AWS (Kafka + CENTRAL + SQLite) · PC de estaciones (WS_01, WS_02) · PC de operarios (fo1, fo2).
Todo corre en Docker. Para entender *cómo funciona* cada cosa: [GUIA.md](GUIA.md).

En toda la guía, **`<IP>`** es la IP pública actual de la EC2.

---

## 1. AWS: Kafka + CENTRAL

1. **Learner Lab → Start Lab** y esperar el punto verde. En **EC2 → Instances**, la instancia debe estar *Running* con *Status check* **2/2**. Apuntar la **IP pública**.
2. **Connect → EC2 Instance Connect**, usuario **`ubuntu`**. Si falla desde un PC de clase, hacerlo desde un portátil.
3. En la EC2:
   ```bash
   cd ~/SD-Practica1 && git pull
   cd deploy/cloud
   echo "KAFKA_PUBLIC_HOST=$(curl -s https://checkip.amazonaws.com)" > .env
   docker compose up -d --build
   docker compose ps
   ```
    `broker` → *Up (healthy)* · `wm-central` → *Up*. Si sale *permission denied*, poner `sudo` delante de `docker`.
4. Abrir el panel: **`http://<IP>:8080`**.  Las estaciones de la BD aparecen en **gris (Disconnected)**.

> **Por qué el `.env`:** Kafka le dice a cada cliente "háblame en esta IP". La IP de la EC2 cambia en cada sesión del Learner Lab: si el `.env` tiene una vieja, los PCs conectan una vez y luego fallan. El comando la pone sola.

**Pantalla de CENTRAL** (en la EC2): `docker logs -f wm-central`

---

## 2. PC de estaciones (PC LAB 2)

```bash
git clone https://github.com/amf162-ua/SD-Practica1.git
cd SD-Practica1/deploy/lab
copy .env.example .env
```
Editar `.env`:
```text
CENTRAL_HOST=<IP>
CENTRAL_PORT=5000
KAFKA_HOST=<IP>
KAFKA_PORT=9092
```
Construir imágenes (**antes** de la revisión):
```bash
docker compose -f compose.ws.yml build
```
Arrancar (**delante** del profesor):
```bash
docker compose -f compose.ws.yml up -d
```
 En ≤ 10 s, **WS_01** y **WS_02** pasan de gris a **verde** en el panel.

---

## 3. PC de operarios (PC LAB 1)

Igual que el paso 2 (`git clone`, `cd SD-Practica1/deploy/lab`, `copy .env.example .env` con la misma IP), y después:
```bash
docker compose -f compose.fo.yml build
```
Arrancar **cuando las estaciones ya estén en verde** (fo2 empieza a pedir riegos nada más arrancar):
```bash
docker compose -f compose.fo.yml up -d
```

---

## 4. Pantallas

| Pantalla | Comando | Tipo |
|---|---|---|
| Panel de CENTRAL | `http://<IP>:8080` | Navegador |
| Log de CENTRAL | `docker logs -f wm-central` (en la EC2) | Lectura |
| Monitor WS_01 | `docker logs -f ws1-monitor` | Lectura |
| **Engine WS_01** | `docker attach ws1-engine` | **Menú** |
| **Operario OP_01** | `docker attach fo1` | **Menú** |
| Operario OP_02 | `docker logs -f fo2` | Lectura (fichero de servicios) |

**Reglas de `docker attach`:**
- Al entrar no se ve nada → **pulsar Enter** y sale el menú.
- Salir **sin apagar**: **Ctrl+P y luego Ctrl+Q**.
- **Ctrl+C lo apaga** (sirve para simular un *crash*). Volver a arrancarlo y entrar: `docker start -ai <nombre>`.

**Menú Engine:** `r 10` riego manual · `p` parar · `k` simular fuga · `o` fuga resuelta · `e` estado
**Menú operario:** `1` pedir riego · `2` ver estaciones y últimos riegos · `3` fichero de servicios · `0` salir

---

## 5. Guion de la demostración (según la guía de corrección)

### 5.1 Funcionamiento base

| # | Qué pide la guía | Qué hacer | Qué se ve |
|---|---|---|---|
| B1 | Todo arranca y funciona **sin interacción** | `docker logs -f fo2` | 3 riegos: *recibida → apertura → AUTORIZADA → telemetría → RESUMEN*, 4 s de pausa entre ellos. El 4.º (WS_99) **DENEGADA**. Repetir: `docker restart fo2` |
| B2 | Al reiniciar, CENTRAL carga las WS de la BD en gris | EC2: `docker restart wm-central` | Log: *N estaciones cargadas de la BD (DESCONECTADAS)*; en segundos vuelven a verde |
| B3 | Riego pedido por un FO | fo1: `1` → `WS_01` → `20` | fo1: pasos y telemetría. Panel: **verde parpadeando** con caudal, volumen y operario. Al final, RESUMEN y fila en *Últimos riegos* |
| B4 | Validar disponibilidad | Durante B3, fo1 (u OP_03) pide `WS_01` | **DENEGADA: … (estado: REGANDO)** |
| B5 | Riego desde la propia WS | Engine: `r 15`, y después `p` | Panel: operario `WS_01-LOCAL`; motivo **PARADA_MANUAL** |
| B6 | Órdenes de CENTRAL a una o a todas | Panel: *Iniciar riego* en WS_02 → destino *Todas* → **Bloquear** → **Activar** | Bloquear: todas **naranja "Out of Service"** y el riego se corta (BLOQUEO_CENTRAL). Activar: **verde** |
| B7 | Fuga detectada por la WS | Con WS_01 regando, Engine: `k` | Riego cortado, panel **rojo (Leak)**, resumen con motivo FUGA. Engine: `o` → **verde** |
| B8 | Autenticación con denegación | `WS_PIRATA` (sección 6) | CENTRAL: *Rechazada WS_PIRATA: estación NO registrada* |
| B9 | Operario no registrado | `OP_99` (sección 6), pedir riego | **DENEGADA: El operario OP_99 no está dado de alta**. Darlo de alta en el panel → ya se autoriza |
| B10 | El FO ve todas las WS | fo1: `2` | Estado de todas las estaciones y últimos riegos |

### 5.2 Resiliencia (caídas abruptas)

`docker stop` y Ctrl+C equivalen a un *crash*.

| # | Qué cae | Qué hacer | Qué se ve | Al volver |
|---|---|---|---|---|
| R1 | **Monitor** | Durante un riego: `docker stop ws1-monitor` | Panel **gris** en ≤ 6 s, el riego termina. Engine: *Estación AVERIADA*; `r 5` se rechaza | `docker start ws1-monitor` → verde |
| R2 | **Engine** | Durante un riego: `docker stop ws1-engine` | Monitor: *Conexión con el Engine perdida* (o *El Engine no responde*); panel **rojo** en ≤ 3 s; el FO recibe resumen | `docker start ws1-engine` → *Riego interrumpido… Enviando su estado final*; en la BD el motivo pasa a **AVERIA**; verde |
| R3 | **Operario** | fo1 pide 20 s y pulsar **Ctrl+C** | El riego sigue en el panel | `docker start -ai fo1` → **RESULTADO DE UN SERVICIO ANTERIOR** |
| R4 | **CENTRAL** | EC2, durante un riego: `docker stop wm-central` | El riego sigue. Monitor: *Imposible conectar con WM_Central*. FO: aviso a los 6 s y no deja pedir riegos | `docker start wm-central` → el riego aparece en la BD; FO: *Conexión restablecida* |
| R5 | **Kafka** | EC2: `docker stop broker` | Todos: *Conexión con el broker perdida* (sin errores) | `docker start broker` → en ~15 s todo funciona **sin reiniciar nada** |
| R6 | Nuevas instancias | Lanzar WS_03 y OP_03 (sección 6) | Aparecen y funcionan sin afectar al resto | — |

---

## 6. Nuevas instancias sobre la marcha

Requieren que `compose.ws.yml` esté levantado (crea la red `wm-ws_default` y las imágenes).

**Estación nueva WS_03** (`--registrar` la da de alta en CENTRAL):
```bash
docker run -d --name ws3-monitor --network wm-ws_default wm-ws python -u WM_WS/WM_WS_M.py 6000 <IP> 5000 WS_03 Plaza de los Luceros --registrar
docker run -dit --name ws3-engine --network wm-ws_default wm-ws python -u WM_WS/WM_WS_E.py <IP> 9092 ws3-monitor 6000
docker attach ws3-engine
```
**Estación no registrada** (se rechaza; salir con Ctrl+C):
```bash
docker run --rm wm-ws python -u WM_WS/WM_WS_M.py 6000 <IP> 5000 WS_PIRATA Calle Falsa
```
**Operario nuevo** (`OP_03` existe; `OP_99` no):
```bash
docker run -it --name fo3 wm-fo python -u WM_FO/WM_FO.py <IP> 9092 OP_03
docker run --rm -it wm-fo python -u WM_FO/WM_FO.py <IP> 9092 OP_99
```
> Sin `--rm` el contenedor sobrevive a `docker stop` y se puede volver a arrancar (para simular caídas). Limpiar al final: `docker rm -f ws3-monitor ws3-engine fo3`

---

## 7. Si algo falla

| Síntoma | Causa → solución |
|---|---|
| Estaciones siempre **grises**; Monitor: *Imposible conectar… timed out* | IP del `.env` del PC incorrecta, o la EC2 cambió de IP → corregir `.env` y `docker compose -f compose.ws.yml up -d` |
| El operario no muestra el menú | `docker logs --tail 20 fo1`. Si repite *Broker … no disponible*: IP o `KAFKA_PUBLIC_HOST` de la EC2 incorrectos |
| Cambié el `.env` y no hace caso | `docker start` no relee el `.env`: usar `docker compose … up -d` |
| `docker attach` en blanco | Pulsar Enter |
| *No such network: wm-ws_default* | Levantar antes `compose.ws.yml` |
| *Container name … already in use* | `docker rm -f <nombre>` |
| Panel no carga | Learner Lab apagado, IP cambiada o puerto 8080 cerrado en el Security Group |

Puertos que debe tener abiertos el Security Group de la EC2: **22, 5000, 8080, 9092**.

---

## 8. Apagar

- **PCs de clase** (desde `deploy/lab`): `docker compose -f compose.fo.yml down` y `docker compose -f compose.ws.yml down`. Borrar `.env` y la carpeta del repo; cerrar sesión.
- **AWS:** *End Lab*. Los contenedores arrancan solos al volver y la BD se conserva.  **Nunca** `docker compose down -v` (borra la BD).

---

## Ensayo en un solo PC

Igual que arriba, pero todo en tu ordenador: levanta `deploy/cloud` en local y usa como `<IP>` la de tu PC en la red (`ipconfig`, **no** `localhost`). El panel estará en `http://localhost:8080` y las pantallas de CENTRAL con `docker logs -f wm-central`.
