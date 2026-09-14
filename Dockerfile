FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY producer.py consumer.py .

# Por defecto no ejecuta un script fijo; el compose definirá el comando
CMD ["python"]