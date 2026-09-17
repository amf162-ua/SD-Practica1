# Water Management System (SD)

Sistema distribuido de monitoreo y gestión de estaciones de agua en tiempo real desarrollado en Python utilizando Sockets TCP persistentes y SQLite.

---

## Estructura del Proyecto

* **`WM_Central.py`**: Servidor central que gestiona el registro de estaciones de monitoreo y persiste su estado en la base de datos.
* **`WM_WS_M.py`**: Monitor de la Estación de Agua (*Water Station Monitor*). Actúa como cliente TCP conectado a la CENTRAL y como servidor socket interno para el ENGINE.
* **`WM_WS_E.py`**: Motor de la Estación (*Water Station Engine*). Simula los sensores de la estación respondiendo peticiones periódicas (`PING`) con estado de salud (`OK` o `KO`).
* **`database.py`**: Módulo de abstracción para la gestión de la base de datos SQLite local (`water_management.db`).

---

### 1. Iniciar CENTRAL
Sintaxis:
```bash
python WM_Central.py <puerto_escucha>
```

Ejemplo:
```bash
python WM_Central.py 5000
```

### 2. Iniciar MONITOR
Sintaxis:
```bash
python WM_WS_M.py <puerto_escucha_engine> <ip_central> <puerto_central> <ws_id>
```

Ejemplo:
```bash
python WM_WS_M.py 6000 localhost 5000 WS_01
```

### 3. Iniciar ENGINE
Sintaxis:
```bash
python WM_WS_E.py <ip_monitor> <puerto_monitor>
```

Ejemplo:
```bash
python WM_WS_E.py localhost 6000
```

---
