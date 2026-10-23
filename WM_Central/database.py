import sqlite3
import os

DB_PATH = os.environ.get("DB_PATH", "water_management.db")

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
    # Requisito PDF: Al arrancar, todas las estaciones conocidas pasan a DESCONECTADA
    cursor.execute("UPDATE estaciones SET estado = 'DESCONECTADA'")
    conn.commit()
    conn.close()

def guardar_o_actualizar_estacion(ws_id, ubicacion="Parque Central", estado="DESCONECTADA"):
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
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, ubicacion, estado FROM estaciones")
    filas = cursor.fetchall()
    conn.close()
    return filas