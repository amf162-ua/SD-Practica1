import sqlite3

DB_PATH = "water_management.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS estaciones (
            id TEXT PRIMARY KEY,
            ubicacion TEXT NOT NULL,
            estado TEXT NOT NULL DEFAULT 'DESCONECTADA'
        )
    ''')
    conn.commit()
    conn.close()

def guardar_o_actualizar_estacion(ws_id, ubicacion="Parque Central", estado="DESCONECTADA"):
    """
    Inserta la estación si no existe, o actualiza su estado si ya está registrada.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO estaciones (id, ubicacion, estado)
        VALUES (?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET estado=excluded.estado
    ''', (ws_id, ubicacion, estado))
    conn.commit()
    conn.close()

def obtener_estaciones():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, ubicacion FROM estaciones")
    filas = cursor.fetchall()
    conn.close()
    return filas