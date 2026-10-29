# Se construye desde la raíz del repositorio:  docker build -f WM_FO/Dockerfile .
FROM python:3.12-slim
WORKDIR /app
# Hora de España en los logs (formato POSIX: no necesita el paquete tzdata)
ENV TZ=CET-1CEST,M3.5.0,M10.5.0/3 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ common/
COPY WM_FO/ WM_FO/
