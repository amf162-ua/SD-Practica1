# Water Management System (SD)

Sistema distribuido de simulación para la gestión centralizada del riego en parques y jardines urbanos. Utiliza Sockets TCP persistentes con un protocolo estándar a medida y almacenamiento en base de datos SQLite.

---

## Estructura del Proyecto

* **`WM_Central.py`**: Servidor central que gestiona el registro de estaciones, maneja la detección de fugas en tiempo real y persiste el estado de la red en la base de datos local.
* **`WM_WS_M.py`**: Monitor de la Estación de Riego (*Water Station Monitor*). Actúa como cliente TCP persistente conectado a la CENTRAL y como servidor asíncrono para su ENGINE asociado. Supervisa el estado de salud y notifica incidencias.
* **`WM_WS_E.py`**: Motor de la Estación (*Water Station Engine*). Simula el hardware (caudalímetro y electroválvula) respondiendo a peticiones periódicas (`PING`) y permitiendo inyectar averías interactivas.
* **`database.py`**: Módulo de abstracción para la gestión de la base de datos SQLite local (`water_management.db`).
* **`protocolo.py`**: Implementación del estándar de comunicación `<STX><DATA><ETX><LRC>`. Garantiza la integridad de las tramas en la red mediante cálculo de paridad (XOR byte a byte) y gestión estricta de acuses de recibo (`ENQ`, `ACK`, `NACK`).

---

## Ejecución y Despliegue

Para desplegar la red básica de simulación, ejecuta los módulos en el siguiente orden desde terminales independientes.

### 1. Iniciar CENTRAL
Queda a la escucha de nuevas conexiones de los monitores.
* **Sintaxis**:
  ```bash
  python WM_Central.py <puerto_escucha> <ip_broker_kafka> <puerto_broker>
  ```

Ejemplo:
```bash
python WM_Central.py 5000 localhost 9092
```

### 2. Iniciar MONITOR
Sintaxis:
```bash
python WM_WS_M.py <puerto_servidor_engine> <ip_central> <puerto_central> <ws_id>
```

Ejemplo:
```bash
python WM_WS_M.py 6000 localhost 5000 WS_01
```

### 3. Iniciar ENGINE
Sintaxis:
```bash
python WM_WS_E.py <ip_broker_kafka> <puerto_broker> <ip_monitor> <puerto_monitor>
```

Ejemplo:
```bash
python WM_WS_E.py localhost 9092 localhost 6000
```

---
## Interacción y Simulación de Averías

Una vez levantada la red, el sistema entra en monitorización continua. Desde la terminal del **ENGINE**, puedes interactuar para evaluar la tolerancia a fallos del sistema:

* Escribir **`ko`**: Simula una avería o fuga en la estación de riego. El Monitor lo detectará en el siguiente PING y enviará una alerta `LEAK#` empaquetada a la Central, actualizando la base de datos al estado "FUGA".
* Escribir **`ok`**: Repara la avería y restablece la estación a un estado de funcionamiento normal.
