# Se construye desde la raíz del repositorio:  docker build -f WM_Central/Dockerfile .
FROM python:3.12-slim
WORKDIR /app
# Hora de España en los logs (formato POSIX: no necesita el paquete tzdata)
ENV TZ=CET-1CEST,M3.5.0,M10.5.0/3 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ common/
COPY WM_Central/ WM_Central/

ENV PUERTO_SOCKETS=5000 KAFKA_BROKER=broker:29092 PUERTO_PANEL=8080
EXPOSE 5000 8080
VOLUME /data
CMD python -u WM_Central/WM_Central.py $PUERTO_SOCKETS $KAFKA_BROKER --panel $PUERTO_PANEL --db /data/water_management.db
