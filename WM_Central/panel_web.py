"""
Panel web de CENTRAL (cuadro de mandos de la Figura 2 del enunciado).

  GET  /                 -> página del panel (panel.html)
  GET  /api/estado       -> JSON con estaciones, operarios, últimos riegos y registro
  POST /api/orden        -> {"orden": "REGAR"|"BLOQUEAR"|"ACTIVAR", "ws_id": "WS_01"|"*", "duracion": 10}
  POST /api/estaciones   -> {"id": "WS_05", "ubicacion": "Parque X"}
  POST /api/operarios    -> {"id": "OP_05", "nombre": "Ana"}

La página consulta /api/estado cada segundo, así se ve el riego en tiempo real.
"""

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RUTA_HTML = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'panel.html')


def iniciar_panel(central, puerto: int):
    class Manejador(BaseHTTPRequestHandler):
        def log_message(self, *args):   # sin log de cada petición HTTP
            pass

        def _responder(self, codigo, cuerpo: bytes, tipo='application/json'):
            self.send_response(codigo)
            self.send_header('Content-Type', f'{tipo}; charset=utf-8')
            self.send_header('Content-Length', str(len(cuerpo)))
            self.end_headers()
            self.wfile.write(cuerpo)

        def _json(self, codigo, datos):
            self._responder(codigo, json.dumps(datos).encode('utf-8'))

        def do_GET(self):
            if self.path == '/':
                with open(RUTA_HTML, 'rb') as f:
                    self._responder(200, f.read(), 'text/html')
            elif self.path == '/api/estado':
                self._json(200, {'estaciones': central.foto_estaciones(),
                                 'operarios': central.bd.operarios(),
                                 'riegos': central.bd.ultimos_riegos(),
                                 'registro': list(central.registro)[-80:],
                                 'puerto_sockets': central.puerto_sockets,
                                 'kafka': central.servidor_kafka})
            else:
                self._json(404, {'ok': False, 'mensaje': 'No encontrado'})

        def do_POST(self):
            try:
                datos = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))) or b'{}')
                if self.path == '/api/orden':
                    mensaje = central.orden_central(datos['orden'], datos['ws_id'], int(datos.get('duracion', 10)))
                elif self.path == '/api/estaciones':
                    central.alta_estacion(datos['id'].strip(), datos['ubicacion'].strip())
                    mensaje = f"Estación {datos['id']} registrada"
                elif self.path == '/api/operarios':
                    central.alta_operario(datos['id'].strip(), datos['nombre'].strip())
                    mensaje = f"Operario {datos['id']} registrado"
                else:
                    return self._json(404, {'ok': False, 'mensaje': 'No encontrado'})
                self._json(200, {'ok': True, 'mensaje': mensaje})
            except (KeyError, ValueError, AttributeError) as e:
                self._json(400, {'ok': False, 'mensaje': f'Petición no válida: {e}'})

    servidor = ThreadingHTTPServer(('0.0.0.0', puerto), Manejador)
    central.log(f"[PANEL] Panel web en http://localhost:{puerto}")
    servidor.serve_forever()
