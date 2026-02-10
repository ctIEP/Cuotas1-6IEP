# Usamos una imagen base de Python ligera
FROM python:3.10-slim

# Evitar que Python genere archivos .pyc
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Instalar dependencias del sistema y el Driver de SQL Server
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev \
    && apt-get clean

# Directorio de trabajo
WORKDIR /app

# Copiar archivos
COPY . /app/

# Instalar librerías de Python
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto que Render usa por defecto
EXPOSE 10000

# Comando para arrancar la app
CMD ["python", "main.py"]