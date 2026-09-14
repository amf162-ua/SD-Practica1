import socket
import threading

HOST = 'localhost'
PORT = 5050
FORMAT = 'utf-8'


class WM_Central:

    def __init__(self, host=HOST, port=PORT):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def handle_client(self, conn, addr):
        print(f"\n[NUEVA CONEXIÓN] Estación conectada desde {addr}")
        try:
            # Recepción de la trama enviada por la estación
            pet = conn.recv(4096)
            if pet:
                trama_recibida = pet.decode(FORMAT).strip()
                print(f"[TRAMA RECIBIDA] {trama_recibida}")

                # Parsear campos utilizando el separador #
                campos = trama_recibida.split('#')

                # Validar la trama: REGISTRO#ID#UBICACION
                if len(campos) == 3 and campos[0] == "REGISTRO":
                    id_estacion = campos[1]
                    ubicacion = campos[2]

                    print(
                        f"[REGISTRO OK] ID: {id_estacion} | Ubicación: {ubicacion}"
                    )

                    # Respuesta estructurada al cliente
                    respuesta = "STATUS#OK#Estacion registrada correctamente"
                else:
                    print("[REGISTRO ERROR] Trama malformada.")
                    respuesta = "STATUS#ERROR#Trama de registro invalida"

                # Envío de la confirmación mediante el flujo de salida
                conn.send(respuesta.encode(FORMAT))

        except Exception as e:
            print(f"[ERROR] Error procesando la estación {addr}: {e}")
        finally:
            conn.close()
            print(f"[CONEXIÓN CERRADA] Estación {addr}")

    def start(self):
        # socket.SOL_SOCKET / SO_REUSEADDR evita errores de puerto bloqueado al reiniciar rápidamente
        self.server_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
        )
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()

        # Timeout de 1 segundo para permitir interrupción con Ctrl+C sin congelar la consola
        self.server_socket.settimeout(1.0)

        print(f"[WM_Central] Servidor a la escucha en {self.host}:{self.port}")
        print("[INFO] Presiona Ctrl+C para detener el servidor.\n")

        while True:
            try:
                conn, addr = self.server_socket.accept()
                conn.settimeout(None)

                # Servidor concurrente con hilos (Threads)
                thread = threading.Thread(
                    target=self.handle_client, args=(conn, addr)
                )
                thread.start()

            except socket.timeout:
                continue
            except KeyboardInterrupt:
                print("\n[APAGANDO] Deteniendo WM_Central por Ctrl+C...")
                break

        self.server_socket.close()
        print("[FINALIZADO] Servidor WM_Central detenido con éxito.")


if __name__ == "__main__":
    central = WM_Central()
    central.start()