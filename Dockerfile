FROM python:3.10-slim

WORKDIR /app

# 1. Copia el requirements.txt que está en la raíz
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copia todo el contenido de la raíz al contenedor
COPY . .

# 3. Expone los puertos del tutorial y de la Central
EXPOSE 8080
EXPOSE 5000

# 4. Inicia la Central indicando la subcarpeta WM_Central/
CMD ["python", "-u", "WM_Central/WM_Central.py", "5000", "127.0.0.1", "9092"]