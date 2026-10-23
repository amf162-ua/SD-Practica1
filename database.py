import sqlite3

DB_PATH = "water_management.db"

def init_db():
    """Crea la tabla si no existe y fuerza a 'DESCONECTADA' todas las estaciones al arrancar."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS estaciones (
            id TEXT PRIMARY KEY,
            ubicacion TEXT NOT NULL,
            estado TEXT NOT NULL DEFAULT 'DESCONECTADA'
        )
    ''')
    # Requisito PDF Punto 1: Al arrancar o reiniciar Central, las estaciones se muestran como DESCONECTADA
    cursor.execute("UPDATE estaciones SET estado = 'DESCONECTADA'")
    conn.commit()
    conn.close()

def guardar_o_actualizar_estacion(ws_id, ubicacion="Parque Central", estado="DESCONECTADA"):
    """
    Inserta la estación si no existe, o actualiza su estado y ubicación si ya está registrada.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO estaciones (id, ubicacion, estado)
        VALUES (?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET 
            estado = excluded.estado,
            ubicacion = CASE 
                WHEN excluded.ubicacion != 'Parque Central' THEN excluded.ubicacion 
                ELSE estaciones.ubicacion 
            END
    ''', (ws_id, ubicacion, estado))
    conn.commit()
    conn.close()

def obtener_estaciones():
    """Obtiene todas las estaciones guardadas en la BD."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, ubicacion, estado FROM estaciones")
    filas = cursor.fetchall()
    conn.close()
    return filas