# Guía de Despliegue y Comandos de Examen (Día de la Revisión)

Guía directa para ejecutar en el laboratorio con 3 PCs conectados a AWS, incluyendo todos los comandos de prueba, creación dinámica de módulos, caídas y casos de rúbrica.

---

## 0. Preparación Previa (Antes de entrar al aula)
1. Iniciar **AWS Learner Lab** y copiar la **IP pública de la EC2** (ej. `54.x.y.z`).
2. Si la IP ha cambiado respecto a la anterior:
   - En la EC2: editar `deploy/cloud/.env` $\to$ poner la nueva IP en `KAFKA_PUBLIC_HOST`.
   - En la EC2 ejecutar: `docker compose up -d`.
3. Comprobar en el navegador (móvil o PC):
   ```text
   http://<IP_EC2>:8080
   ```
   Deben verse las estaciones iniciales cargadas de SQLite en **Gris (Disconnected)**.

---

## 🖥️ PC 1: Cuadro de Mandos (CENTRAL)
*Este equipo solo necesita el navegador web para supervisar y gobernar el sistema.*

### Visualización y Órdenes
1. **Abrir panel en pantalla completa (`F11`):**
   ```text
   http://<IP_EC2>:8080
   ```
2. **Órdenes de Central (Punto 11):**
   - **Bloquear:** Seleccionar "Todas las estaciones" $\to$ clic en **Bloquear** $\to$ Todas pasan a **Naranja ("Out of Service")**.
   - **Activar:** Seleccionar "Todas las estaciones" $\to$ clic en **Activar** $\to$ Todas vuelven a **Verde (Available)**.
   - **Iniciar riego:** Seleccionar una WS $\to$ duración `10` $\to$ clic en **Iniciar riego**.
3. **Alta de estación desde la web:** Formulario lateral $\to$ ID: `WS_03`, Ubicación: `Plaza de los Luceros` $\to$ botón **Alta** (aparece en Gris en el panel).

### Comandos útiles en la EC2 (por SSH si se requiere):
- **Ver logs de Central en tiempo real:**
  ```bash
  docker logs -f wm-central
  ```
- **Simular reinicio de Central:**
  ```bash
  docker restart wm-central
  ```
  *(Al arrancar, recarga de SQLite en Gris y los monitores se reconectan solos a los pocos segundos).*

---

## 🖥️ PC 2: Estaciones de Riego (WM_WS)

### 1. Configuración y Arranque Estándar
1. Entrar en la carpeta:
   ```bash
   cd deploy/lab
   ```
2. Configurar en `.env`:
   ```env
   CENTRAL_HOST=<IP_EC2>
   CENTRAL_PORT=5000
   KAFKA_HOST=<IP_EC2>
   KAFKA_PORT=9092
   ```
3. Lanzar las estaciones (`WS_01` y `WS_02`):
   ```bash
   docker compose -f compose.ws.yml up -d --build
   ```
   *(En PC 1 pasan solas de Gris a Verde).*

### 2. Control Interactivo del Engine (`WS_01`)
```bash
docker attach ws1-engine
```
*(Pulsar `Enter` para ver menú. Salir sin apagar: `Ctrl + P` y luego `Ctrl + Q`).*
- `r 15` $\to$ Solicitar riego manual de 15s desde la propia estación (operario `WS_01-LOCAL`).
- `p` $\to$ Parar el riego en curso inmediatamente (parada manual).
- `k` $\to$ **Simular fuga/avería** $\to$ Cierra electroválvula, Central pasa a **Rojo (Leak)** y envía resumen al FO.
- `o` $\to$ **Avería resuelta** $\to$ La estación vuelve a **Verde (Available)** sola.
- `e` $\to$ Ver estado local de la estación.

### 3. Crear Estaciones Dinámicas sobre la Marcha (Escalabilidad)
- **Crear una estación nueva (`WS_03`) con auto-registro:**
  ```bash
  # 1. Monitor WS_03:
  docker run --rm -d --name ws3-monitor --network wm-ws_default wm-ws python -u WM_WS/WM_WS_M.py 6000 <IP_EC2> 5000 WS_03 "Plaza de los Luceros" --registrar

  # 2. Engine WS_03:
  docker run --rm -d --name ws3-engine --network wm-ws_default wm-ws python -u WM_WS/WM_WS_E.py <IP_EC2> 9092 ws3-monitor 6000
  ```
  *(Aparece directamente en Verde en el panel de Central).*

- **Crear una estación FALSA / No registrada (Demostrar rechazo de Central):**
  ```bash
  docker run --rm -it wm-ws python -u WM_WS/WM_WS_M.py 6000 <IP_EC2> 5000 WS_PIRATA
  ```
  *(Central rechaza la conexión con `['WS_NO_REGISTRADA']`. Salir con `Ctrl + C`).*

### 4. Pruebas de Resiliencia y Crashes en PC 2
- **Simular caída súbita del Monitor:**
  ```bash
  docker stop ws1-monitor
  ```
  *(PC 1 pasa a Gris en $\le 6$s. El Engine marca `AVERIADA` y deniega riegos manuales).*
  - Restaurar: `docker start ws1-monitor` $\to$ Vuelve a Verde.
- **Simular caída súbita del Engine durante un riego:**
  ```bash
  docker stop ws1-engine
  ```
  *(Monitor detecta fallo en 3s; Central marca Rojo; los litros regados se salvan en SQLite y resumen al FO).*
  - Restaurar: `docker start ws1-engine` $\to$ Vuelve a Verde.

### 5. Apagar todo en PC 2
```bash
docker compose -f compose.ws.yml down
# Si se levantó WS_03 suelta:
docker stop ws3-engine ws3-monitor 2>/dev/null
```

---

## 🖥️ PC 3: Operarios de Campo (WM_FO)

### 1. Configuración y Arranque Estándar
1. Entrar en la carpeta:
   ```bash
   cd deploy/lab
   ```
2. Configurar en `.env`:
   ```env
   KAFKA_HOST=<IP_EC2>
   KAFKA_PORT=9092
   ```
3. Lanzar los operarios (`fo1` interactivo y `fo2` batch):
   ```bash
   docker compose -f compose.fo.yml up -d --build
   ```

### 2. Demostración Autónoma Inicial (`fo2`)
```bash
docker logs -f fo2
```
- Muestra la ejecución desatendida de `servicios.txt`: solicitudes, telemetría segundo a segundo, pausas de 4s, resúmenes y denegación de `WS_99`.
- *(Salir del visor: `Ctrl + C`).*
- **Re-ejecutar la demostración batch** (si el profesor quiere verla de nuevo):
  ```bash
  docker restart fo2
  ```

### 3. Operario Interactivo (`fo1`)
```bash
docker attach fo1
```
*(Pulsar `Enter` para ver menú. Salir sin apagar: `Ctrl + P` y luego `Ctrl + Q`).*
- Opción `1` $\to$ Solicitar riego: ID estación (ej. `WS_01`), duración (ej. `15`).
- Opción `2` $\to$ Ver todas las estaciones de la red y tabla de últimos riegos registrados en la BD.
- Opción `3` $\to$ Ejecutar otro fichero de servicios.

### 4. Crear Operarios Dinámicos sobre la Marcha
- **Lanzar un operario extra registrado (`OP_03`):**
  ```bash
  docker run --rm -it wm-fo python -u WM_FO/WM_FO.py <IP_EC2> 9092 OP_03
  ```
- **Lanzar un operario FALSO / No registrado (`OP_99`):**
  ```bash
  docker run --rm -it wm-fo python -u WM_FO/WM_FO.py <IP_EC2> 9092 OP_99
  ```
  *(Al pedir un riego con opción 1 en WS_01, Central responde: `DENEGADA: El operario OP_99 no está dado de alta en CENTRAL`).*
  - Si en PC 1 se da de alta `OP_99` en el formulario web, al repetir la petición ya se autoriza.

### 5. Pruebas de Resiliencia en PC 3
- **Caída de operario a mitad de riego:**
  - Mientras riega en `fo1`, pulsar `Ctrl + C` para tumbarlo.
  - La estación en PC 2 sigue regando sola hasta el final.
  - Al terminar, revivir al operario:
    ```bash
    docker start fo1
    docker attach fo1
    ```
  - Pulsar opción `2` $\to$ En pantalla aparece el resultado completado de su servicio guardado en la BD.

### 6. Apagar todo en PC 3
```bash
docker compose -f compose.fo.yml down
```
