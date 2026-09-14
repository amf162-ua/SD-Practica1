import socket
import sys

HOST = 'localhost'
PORT = 5050
FORMAT = 'utf-8'


class WM_WS_M:

    def __init__(
        self, id_estacion="WS-04", ubicacion="River Park", host=HOST, port=PORT
    ):
        self.id_estacion = id_estacion
        self.ubicacion = ubicacion
        self.host = host
        self.port = port
        self.socket_cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def ejecutar(self):
        print(
            f"[WM_WS_M] Iniciando Estación de Riego {self.id_estacion}..."
        )

        try:
            self.socket_cliente.connect((self.host, self.port))
            print(
                f"[WM_WS_M] Conectado a WM_Central en {self.host}:{self.port}"
            )

            # Construir trama estructurada: REGISTRO#WS-04#River Park
            trama_registro = (
                f"REGISTRO#{self.id_estacion}#{self.ubicacion}"
            )

            # Envío de la trama
            print(f"[ENVIANDO TRAMA] {trama_registro}")
            self.socket_cliente.send(trama_registro.encode(FORMAT))

            # Lectura de la respuesta enviada por la central
            respuesta = self.socket_cliente.recv(4096).decode(FORMAT)
            print(f"[RESPUESTA DE CENTRAL] {respuesta}")

        except KeyboardInterrupt:
            print("\n[CANCELADO] Salida manual por Ctrl+C.")
        except ConnectionRefusedError:
            print(
                f"[ERROR] No se pudo conectar a WM_Central en {self.host}:{self.port}"
            )
        except Exception as e:
            print(f"[ERROR inesperado]: {e}")
        finally:
            # Cierre limpio de la conexión (close())
            self.socket_cliente.close()
            print("[WM_WS_M] Cierre limpio de la conexión finalizado.")
            sys.exit(0)


if __name__ == "__main__":
    monitor = WM_WS_M(id_estacion="WS-04", ubicacion="River Park")
    monitor.ejecutar()