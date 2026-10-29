"""
Protocolo de sockets <STX><DATA><ETX><LRC> usado entre:
  - WM_WS_M  <-> WM_Central  (autenticación y estado de salud de la estación)
  - WM_WS_E  <-> WM_WS_M     (asignación de ID y comprobación de salud cada segundo)

Flujo de una conexión:
    cliente                       servidor
      ENQ  ----------------------->            (¿puedo hablar?)
           <-----------------------  ACK
      <STX>REQUEST<ETX><LRC> ----->
           <-----------------------  ACK / NACK (LRC correcto o no)
           <-----------------------  <STX>ANSWER<ETX><LRC>
      ACK / NACK ----------------->
      ... (más pares REQUEST/ANSWER) ...
      EOT  ----------------------->            (fin de la conexión)

DATA son campos separados por '#':  CODIGO#campo1#...#campoN
LRC  es el XOR byte a byte de DATA.

TCP es un flujo de bytes, no de mensajes: un recv() puede traer media trama o
dos tramas juntas. Por eso CanalTramas guarda lo recibido en un buffer y extrae
los mensajes completos de él, en lugar de suponer "un recv() = un mensaje".
"""

import socket

STX = b'\x02'
ETX = b'\x03'
EOT = b'\x04'
ENQ = b'\x05'
ACK = b'\x06'
NACK = b'\x15'

SEPARADOR = '#'
MAX_REINTENTOS = 3


class ErrorProtocolo(Exception):
    """La otra parte no respeta el protocolo o la conexión se ha cerrado."""


def calcular_lrc(data: bytes) -> int:
    lrc = 0
    for byte in data:
        lrc ^= byte
    return lrc


def empaquetar(mensaje: str) -> bytes:
    data = mensaje.encode('utf-8')
    return STX + data + ETX + bytes([calcular_lrc(data)])


def construir(*campos) -> str:
    return SEPARADOR.join(str(c) for c in campos)


def separar(mensaje: str) -> list:
    return mensaje.split(SEPARADOR)


class CanalTramas:
    """Envoltorio sobre un socket TCP que envía y recibe tramas del protocolo."""

    def __init__(self, sock: socket.socket, timeout: float = None):
        self.sock = sock
        self.sock.settimeout(timeout)
        self.buffer = b''

    # -- Lectura de bajo nivel ------------------------------------------------
    def _rellenar_buffer(self):
        datos = self.sock.recv(1024)
        if not datos:
            raise ErrorProtocolo("Conexión cerrada por la otra parte")
        self.buffer += datos

    def _leer_byte(self) -> bytes:
        while not self.buffer:
            self._rellenar_buffer()
        byte, self.buffer = self.buffer[:1], self.buffer[1:]
        return byte

    def _leer_trama(self):
        """Lee <STX>DATA<ETX><LRC> del buffer. Devuelve (mensaje, lrc_valido)."""
        while True:
            idx_etx = self.buffer.find(ETX)
            if idx_etx != -1 and len(self.buffer) > idx_etx + 1:
                break
            self._rellenar_buffer()
        data = self.buffer[:idx_etx]
        lrc = self.buffer[idx_etx + 1]
        self.buffer = self.buffer[idx_etx + 2:]
        if calcular_lrc(data) != lrc:
            return None, False
        try:
            return data.decode('utf-8'), True
        except UnicodeDecodeError:
            return None, False

    def leer_control(self) -> bytes:
        """Lee el siguiente byte de control (ENQ, ACK, NACK, EOT) o STX."""
        return self._leer_byte()

    # -- Operaciones del protocolo ---------------------------------------------
    def enviar(self, mensaje: str):
        """Envía una trama y espera ACK. Si llega NACK la reenvía (hasta 3 veces)."""
        for _ in range(MAX_REINTENTOS):
            self.sock.sendall(empaquetar(mensaje))
            respuesta = self._leer_byte()
            if respuesta == ACK:
                return
            if respuesta != NACK:
                raise ErrorProtocolo(f"Se esperaba ACK/NACK y llegó {respuesta!r}")
        raise ErrorProtocolo("Demasiados NACK seguidos")

    def recibir(self, primer_byte: bytes = None) -> str:
        """Recibe una trama, responde ACK/NACK y devuelve el mensaje.
        Si ya se leyó el STX (con leer_control) se pasa como primer_byte."""
        for _ in range(MAX_REINTENTOS):
            byte = primer_byte or self._leer_byte()
            primer_byte = None
            if byte == EOT:
                raise ErrorProtocolo("EOT: la otra parte cierra la conexión")
            if byte != STX:
                raise ErrorProtocolo(f"Se esperaba STX y llegó {byte!r}")
            mensaje, valido = self._leer_trama()
            if valido:
                self.sock.sendall(ACK)
                return mensaje
            self.sock.sendall(NACK)
        raise ErrorProtocolo("Demasiadas tramas con LRC incorrecto")

    def peticion(self, mensaje: str) -> str:
        """Lado cliente: envía REQUEST y devuelve el ANSWER."""
        self.enviar(mensaje)
        return self.recibir()

    def saludar(self):
        """Lado cliente: ENQ -> ACK al abrir la conexión."""
        self.sock.sendall(ENQ)
        if self._leer_byte() != ACK:
            raise ErrorProtocolo("El servidor no aceptó el ENQ")

    def esperar_saludo(self):
        """Lado servidor: espera ENQ y responde ACK."""
        if self._leer_byte() != ENQ:
            raise ErrorProtocolo("Se esperaba ENQ al iniciar la conexión")
        self.sock.sendall(ACK)

    def despedir(self):
        """Envía EOT y cierra el socket (sin fallar si ya estaba cerrado)."""
        try:
            self.sock.sendall(EOT)
        except OSError:
            pass
        self.cerrar()

    def cerrar(self):
        try:
            self.sock.close()
        except OSError:
            pass
