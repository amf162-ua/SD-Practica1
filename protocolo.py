# Caracteres de control ASCII estándar
STX = b'\x02'  # Start of Text
ETX = b'\x03'  # End of Text
ENQ = b'\x05'  # Enquiry
ACK = b'\x06'  # Acknowledge
NACK = b'\x15' # Negative Acknowledge
EOT = b'\x04'  # End of Transmission

def empaquetar(mensaje_str):
    """
    Convierte un string en una trama de bytes: <STX><DATA><ETX><LRC>
    El LRC es el XOR byte a byte del campo <DATA>.
    """
    data = mensaje_str.encode('utf-8')
    lrc = 0
    for byte in data:
        lrc ^= byte
    
    # Se ensambla la trama completa
    return STX + data + ETX + bytes([lrc])

def desempaquetar(trama_bytes):
    """
    Extrae el string de una trama <STX><DATA><ETX><LRC> y valida el LRC.
    Retorna (mensaje_str, es_valido).
    """
    if not trama_bytes or len(trama_bytes) < 4:
        return None, False
    
    # Validar que empieza por STX
    if trama_bytes[0:1] != STX:
        return None, False
        
    # Buscar la posición de ETX
    idx_etx = trama_bytes.find(ETX)
    if idx_etx == -1 or idx_etx + 1 >= len(trama_bytes):
        return None, False
        
    # Extraer los datos y el LRC recibido
    data = trama_bytes[1:idx_etx]
    lrc_recibido = trama_bytes[idx_etx + 1]
    
    # Calcular el LRC localmente sobre los datos extraídos
    lrc_calculado = 0
    for byte in data:
        lrc_calculado ^= byte
        
    # Validar si la transmisión fue correcta
    if lrc_calculado == lrc_recibido:
        return data.decode('utf-8'), True
    else:
        return None, False