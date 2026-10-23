# Water Management Network (WM) — Sistemas Distribuidos (26/27)

Sistema distribuido para la gestión y monitorización en tiempo real del riego urbano. Combina **Sockets TCP** (protocolo a medida), **Apache Kafka** (streaming de eventos), **SQLite** (persistencia) y un despliegue híbrido Cloud (**Railway**) / Laboratorio (**Docker**).

---

## 📁 Estructura del Repositorio

```text
SD-Practica1/
├── compose.yml              # Infraestructura Docker Compose local para Kafka
├── launcher_ws.py           # Lanzador GUI para Estaciones de Riego (PC LAB 2)
├── launcher_fo.py           # Lanzador GUI para Operarios de Campo (PC LAB 1)
├── Readme.md                # Guía de despliegue y documentación
├── Nombres.txt              # Identificación del grupo (Nombre, DNI, Grupo)
├── WM_Central/              # Módulo Central (Desplegable en Railway / Cloud)
│   ├── WM_Central.py        # Núcleo servidor, lógica de negocio y GUI Tkinter/Headless
│   ├── database.py         # Persistencia con SQLite (water_management.db)
│   ├── protocolo.py        # Protocolo de Sockets (<STX><DATA><ETX><LRC>)
│   ├── requirements.txt    # Dependencias del servidor (kafka-python)
│   └── Dockerfile          # Configuración de contenedor para CI/CD en Railway
├── WM_WS/                   # Estaciones de Riego (PC LAB 2)
│   ├── WM_WS_M.py          # Monitor de salud de la estación
│   ├── WM_WS_E.py          # Motor de hardware (electroválvula y telemetría)
│   └── protocolo.py        # Protocolo de empaquetado custom
├── WM_FO/                   # Operarios de Campo (PC LAB 1)
│   ├── WM_FO.py            # Aplicación de solicitudes de riego
│   └── servicios.txt       # Fichero de prueba de servicios
└── prompts/                 # Transparencia en el uso de asistentes de IA
    └── prompts_usados.txt
```

---

## ⚙️ Componentes del Sistema

1. **`WM_Central`**:
   - **Sockets (Puerto 5000 / $PORT)**: Atiende autenticación (`AUTH`), estado (`STATUS`) y fugas (`LEAK`).
   - **Kafka**: Gestiona solicitudes (`wm-orders-request`), respuestas (`wm-orders-response`), telemetría (`wm-telemetry`) y alertas (`wm-alerts-events`).
   - **SQLite (`database.py`)**: Persiste estaciones. Al arrancar, todas pasan inicialmente a `DESCONECTADA`.
   - **Panel / Headless**: Colores según estado (**VERDE** disponible, **VERDE PARPADEANTE** regando, **ROJO** fuga, **NARANJA** fuera de servicio / avería, **GRIS** desconectada). Soporta órdenes arbitrarias del Punto 11 (Iniciar riego, Bloquear, Activar).

2. **`WM_WS_M` (Monitor)**:
   - Cliente Socket con Central y servidor local para el Engine. Supervisa la salud de la estación con PINGs cada 1s. Si hay fallo o `KO`, notifica `LEAK` a Central.

3. **`WM_WS_E` (Engine)**:
   - Conectado a Kafka (telemetría/órdenes) y al Monitor por Sockets. Permite inyectar fallos por consola (`ko`, `ok`, `stop`). Si cae la conexión o hay fuga, corta el riego de inmediato.

4. **`WM_FO` (Field Operator)**:
   - Solicita riegos vía Kafka en modo interactivo o por archivo (`servicios.txt`), respetando la pausa obligatoria de 4 segundos entre servicios.

5. **`protocolo.py`**:
   - Empaquetado `<STX><DATA><ETX><LRC>` con verificación XOR y asentimientos `ENQ`/`ACK`/`NACK`/`EOT`.

---

## 🚀 Guía de Despliegue

### 1. Entorno Local (Desarrollo)

- **Arrancar Kafka con Docker**:
  ```bash
  docker-compose up -d
  ```
- **Arrancar CENTRAL**:
  ```bash
  python WM_Central/WM_Central.py 5000 127.0.0.1 9092
  ```
- **Lanzar Estaciones (PC LAB 2)**: `python launcher_ws.py` o mediante terminal:
  ```bash
  python WM_WS/WM_WS_M.py 6001 127.0.0.1 5000 WS_01
  python WM_WS/WM_WS_E.py 127.0.0.1 9092 127.0.0.1 6001 WS_01
  ```
- **Lanzar Operario (PC LAB 1)**: `python launcher_fo.py` o mediante terminal:
  ```bash
  python WM_FO/WM_FO.py 127.0.0.1 9092 OP_01
  ```

---

### 2. Escenario de Evaluación (Railway + Laboratorio)

- **Parte Cloud (Railway)**:
  - **Kafka**: Plantilla `apache/kafka` con **TCP Proxy** expuesto (puerto 9092).
  - **WM_Central**: Desplegada desde `/WM_Central` vía CI/CD con **TCP Proxy** expuesto (puerto 5000).
- **PC LAB 2 (Estaciones)**: Ejecutar `python launcher_ws.py` apuntando a los TCP Proxies de Railway.
- **PC LAB 1 (Operarios)**: Ejecutar `python launcher_fo.py` apuntando al TCP Proxy de Kafka de Railway.

---

## 💻 Comandos del Engine (`WM_WS_E`)

- `ko`: Aborta el riego activo por fuga, pasa a estado **`FUGA` (ROJO)** y notifica a Central.
- `ok`: Restablece la estación a **`DISPONIBLE` (VERDE)**.
- `stop`: Detiene manualmente el riego en curso.