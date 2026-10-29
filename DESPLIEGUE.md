# Guía Rápida de Despliegue (Día de la Revisión)

Guía directa y concisa para desplegar el escenario en el laboratorio utilizando 3 PCs conectados a AWS.

---

## 0. Preparación previa (10 min antes de entrar)
1. Encender AWS Learner Lab y copiar la **IP pública de la EC2** (ej. `54.x.y.z`).
2. Si la IP ha cambiado respecto a la anterior:
   - En la EC2: editar `deploy/cloud/.env` $\to$ poner la nueva IP en `KAFKA_PUBLIC_HOST`.
   - Ejecutar en la EC2: `docker compose up -d`.
3. Comprobar en el navegador (móvil o PC):
   ```text
   http://<IP_EC2>:8080
   ```
   Deben verse las estaciones cargadas de SQLite en **Gris (Disconnected)**.

---

## 🖥️ PC 1: Cuadro de Mandos (CENTRAL)
*No requiere ejecutar código, solo visualizar el panel de control.*

1. Abrir navegador en pantalla completa (`F11`):
   ```text
   http://<IP_EC2>:8080
   ```
2. Dejar visible el panel durante toda la evaluación:
   - Al inicio: estaciones en **Gris**.
   - Al encender PC 2: pasarán solas a **Verde**.
   - Durante riego: parpadearán en **Verde (Watering)** con caudal y volumen.
   - En fuga: pasarán a **Rojo (Leak)**.
   - Si se pulsa "Bloquear": pasarán a **Naranja ("Out of Service")**.

---

## 🖥️ PC 2: Estaciones de Riego (WM_WS)
*Despliega los contenedores de las estaciones WS_01 y WS_02.*

1. Entrar en la carpeta:
   ```bash
   cd deploy/lab
   ```
2. Editar el archivo `.env`:
   ```env
   CENTRAL_HOST=<IP_EC2>
   CENTRAL_PORT=5000
   KAFKA_HOST=<IP_EC2>
   KAFKA_PORT=9092
   ```
3. Lanzar las estaciones:
   ```bash
   docker compose -f compose.ws.yml up -d --build
   ```
   *(En el PC 1 pasarán de Gris a Verde en 3 segundos).*

4. **Terminal interactiva del Engine (simulador de fugas):**
   ```bash
   docker attach ws1-engine
   ```
   - Pulsa `Enter` para ver el menú.
   - Tecla `k` $\to$ Simular fuga en caliente (pone la WS en Rojo).
   - Tecla `o` $\to$ Resolver avería (vuelve a Verde).
   - Tecla `r 15` $\to$ Riego manual de 15s desde la propia estación.
   - Tecla `p` $\to$ Parada manual de emergencia.
   - *(Salir sin apagar: `Ctrl + P` y luego `Ctrl + Q`).*

---

## 🖥️ PC 3: Operarios de Campo (WM_FO)
*Despliega los contenedores de los operarios fo1 (manual) y fo2 (batch).*

1. Entrar en la carpeta:
   ```bash
   cd deploy/lab
   ```
2. Editar el archivo `.env`:
   ```env
   KAFKA_HOST=<IP_EC2>
   KAFKA_PORT=9092
   ```
3. Lanzar los operarios:
   ```bash
   docker compose -f compose.fo.yml up -d --build
   ```

4. **Demostración autónoma inicial (fo2):**
   ```bash
   docker logs -f fo2
   ```
   - Muestra la ejecución desatendida de `servicios.txt` (pasos, telemetría a 1 Hz, pausas de 4s y resumen).
   - *(Salir del visor: `Ctrl + C`).*

5. **Terminal interactiva del Operario (fo1):**
   ```bash
   docker attach fo1
   ```
   - Pulsa `Enter` para ver el menú.
   - Opción `1` $\to$ Solicitar riego (pide WS y duración).
   - Opción `2` $\to$ Ver estado de la red y tabla de últimos riegos.
   - *(Salir sin apagar: `Ctrl + P` y luego `Ctrl + Q`).*

---

## ⚠️ Comandos Rápidos de Resiliencia (Si el profesor pide caídas)

- **Simular caída del Monitor (PC 2):**
  ```bash
  docker stop ws1-monitor
  ```
  *(PC 1 pasa a Gris en $\le 6$s; Engine queda en Averiada).*
  Restaurar: `docker start ws1-monitor` $\to$ Vuelve a Verde.

- **Simular caída del Engine durante un riego (PC 2):**
  ```bash
  docker stop ws1-engine
  ```
  *(Central marca Rojo en 3s; no se pierden litros en la BD).*
  Restaurar: `docker start ws1-engine` $\to$ Vuelve a Verde.

- **Simular caída del Operario durante un riego (PC 3):**
  ```bash
  docker stop fo1
  ```
  *(La estación sigue regando sola. Al volver a iniciar `fo1`, la opción 2 muestra el resumen completado).*
