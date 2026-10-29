"""
Persistencia de CENTRAL en SQLite.

Tablas:
  estaciones  -> WS dadas de alta (id, ubicación, si están bloqueadas por CENTRAL)
  operarios   -> operarios autorizados a pedir riegos
  riegos      -> histórico de riegos realizados (para consulta y auditoría)

El estado "vivo" de cada WS (conectada, regando, fuga...) no se guarda aquí:
depende de las conexiones activas y CENTRAL lo mantiene en memoria. Lo único
que debe sobrevivir a un reinicio es qué WS existen y si están bloqueadas.
"""

import os
import sqlite3
import threading
import time

OPERARIOS_INICIALES = [('OP_01', 'Operario 1'), ('OP_02', 'Operario 2'),
                       ('OP_03', 'Operario 3'), ('OP_04', 'Operario 4')]

ESTACIONES_INICIALES = [
    ('WS_01', 'Parque de El Palmeral'),
    ('WS_02', 'Jardines de Canalejas')
]


class BaseDatos:
    def __init__(self, ruta: str):
        carpeta = os.path.dirname(ruta)
        if carpeta:
            os.makedirs(carpeta, exist_ok=True)
        # Una única conexión compartida por todos los hilos, protegida con un lock
        self.conn = sqlite3.connect(ruta, check_same_thread=False)
        self.lock = threading.Lock()
        self._migrar_si_procede()
        self._crear_tablas()

    def _migrar_si_procede(self):
        with self.lock, self.conn:
            tablas = [r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            if 'estaciones' in tablas:
                cols = [c[1] for c in self.conn.execute('PRAGMA table_info(estaciones)').fetchall()]
                if 'bloqueada' not in cols:
                    self.conn.execute('ALTER TABLE estaciones ADD COLUMN bloqueada INTEGER NOT NULL DEFAULT 0')
                if 'fecha_alta' not in cols:
                    self.conn.execute('ALTER TABLE estaciones ADD COLUMN fecha_alta TEXT NOT NULL DEFAULT ""')

    def _crear_tablas(self):
        with self.lock, self.conn:
            self.conn.executescript('''
                CREATE TABLE IF NOT EXISTS estaciones (
                    id         TEXT PRIMARY KEY,
                    ubicacion  TEXT NOT NULL,
                    bloqueada  INTEGER NOT NULL DEFAULT 0,
                    fecha_alta TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operarios (
                    id     TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS riegos (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id    TEXT NOT NULL,
                    ws_id       TEXT NOT NULL,
                    operator_id TEXT NOT NULL,
                    fin         TEXT NOT NULL,
                    segundos    INTEGER NOT NULL,
                    volumen_l   REAL NOT NULL,
                    motivo      TEXT NOT NULL
                );
            ''')
            if not self.conn.execute('SELECT 1 FROM operarios LIMIT 1').fetchone():
                self.conn.executemany('INSERT INTO operarios VALUES (?, ?)', OPERARIOS_INICIALES)
            if not self.conn.execute('SELECT 1 FROM estaciones LIMIT 1').fetchone():
                ahora = time.strftime('%Y-%m-%d %H:%M:%S')
                self.conn.executemany(
                    'INSERT INTO estaciones (id, ubicacion, bloqueada, fecha_alta) VALUES (?, ?, 0, ?)',
                    [(ws_id, ubi, ahora) for ws_id, ubi in ESTACIONES_INICIALES]
                )

    # -- Estaciones -------------------------------------------------------------
    def estaciones(self) -> list:
        with self.lock:
            filas = self.conn.execute('SELECT id, ubicacion, bloqueada FROM estaciones ORDER BY id')
            return [{'id': f[0], 'ubicacion': f[1], 'bloqueada': bool(f[2])} for f in filas]

    def existe_estacion(self, ws_id: str) -> bool:
        with self.lock:
            return self.conn.execute('SELECT 1 FROM estaciones WHERE id = ?', (ws_id,)).fetchone() is not None

    def guardar_estacion(self, ws_id: str, ubicacion: str):
        """Da de alta la WS o actualiza su ubicación si ya existía."""
        with self.lock, self.conn:
            self.conn.execute('''
                INSERT INTO estaciones (id, ubicacion, fecha_alta) VALUES (?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET ubicacion = excluded.ubicacion
            ''', (ws_id, ubicacion, time.strftime('%Y-%m-%d %H:%M:%S')))

    def marcar_bloqueada(self, ws_id: str, bloqueada: bool):
        with self.lock, self.conn:
            self.conn.execute('UPDATE estaciones SET bloqueada = ? WHERE id = ?', (int(bloqueada), ws_id))

    # -- Operarios --------------------------------------------------------------
    def operarios(self) -> list:
        with self.lock:
            filas = self.conn.execute('SELECT id, nombre FROM operarios ORDER BY id')
            return [{'id': f[0], 'nombre': f[1]} for f in filas]

    def existe_operario(self, operator_id: str) -> bool:
        with self.lock:
            return self.conn.execute('SELECT 1 FROM operarios WHERE id = ?', (operator_id,)).fetchone() is not None

    def guardar_operario(self, operator_id: str, nombre: str):
        with self.lock, self.conn:
            self.conn.execute('INSERT OR REPLACE INTO operarios VALUES (?, ?)', (operator_id, nombre))

    # -- Riegos -----------------------------------------------------------------
    def guardar_riego(self, order_id, ws_id, operator_id, segundos, volumen_l, motivo):
        with self.lock, self.conn:
            self.conn.execute('''
                INSERT INTO riegos (order_id, ws_id, operator_id, fin, segundos, volumen_l, motivo)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (order_id, ws_id, operator_id, time.strftime('%Y-%m-%d %H:%M:%S'),
                  segundos, volumen_l, motivo))

    def ultimos_riegos(self, limite: int = 10) -> list:
        with self.lock:
            filas = self.conn.execute('''
                SELECT fin, ws_id, operator_id, segundos, volumen_l, motivo
                FROM riegos ORDER BY id DESC LIMIT ?
            ''', (limite,))
            claves = ('fin', 'ws_id', 'operator_id', 'segundos', 'volumen_l', 'motivo')
            return [dict(zip(claves, f)) for f in filas]
