# Protocolo de empaquetado custom según especificación de la práctica
# Formato de trama: <STX><DATA><ETX><LRC>
# LRC = XOR byte a byte del campo DATA

STX = b'\x02'  # Start of Text
ETX = b'\x03'  # End of Text
ENQ = b'\x05'  # Enquiry
ACK = b'\x06'  # Acknowledge
NACK = b'\x15' # Negative Acknowledge
EOT = b'\x04'  # End of Transmission

def empaquetar(mensaje_str: str) -> bytes:
    """Convierte una cadena de texto en trama de bytes con control de errores LRC."""
    data = mensaje_str.encode('utf-8')
    lrc = 0
    for byte in data:
        lrc ^= byte
    return STX + data + ETX + bytes([lrc])

def desempaquetar(trama_bytes: bytes):
    """
    Extrae el mensaje de la trama y valida el LRC.
    Retorna (mensaje_str, es_valido).
    """
    if not trama_bytes or len(trama_bytes) < 4:
        return None, False
    
    if trama_bytes[0:1] != STX:
        return None, False
        
    idx_etx = trama_bytes.find(ETX)
    if idx_etx == -1 or idx_etx + 1 >= len(trama_bytes):
        return None, False
        
    data = trama_bytes[1:idx_etx]
    lrc_recibido = trama_bytes[idx_etx + 1]
    
    lrc_calculado = 0
    for byte in data:
        lrc_calculado ^= byte
        
    if lrc_calculado == lrc_recibido:
        try:
            return data.decode('utf-8'), True
        except UnicodeDecodeError:
            return None, False
    else:
        return None, False