# 1. Imagen base con Python 3.10
FROM python:3.10-slim

# 2. Creamos y nos situamos en la carpeta de trabajo interna del contenedor
WORKDIR /app

# 3. Copiamos el requirements.txt (que está en la raíz) e instalamos librerías
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiamos todo el código de nuestro proyecto al contenedor
COPY . .

# 5. Exponemos los puertos necesarios (8080 para la web de ECS y 5000 para Sockets)
EXPOSE 8080
EXPOSE 5000

# 6. Comando por defecto para iniciar la Central al arrancar el contenedor
CMD ["python", "-u", "WM_Central/WM_Central.py", "5000", "127.0.0.1", "9092"]