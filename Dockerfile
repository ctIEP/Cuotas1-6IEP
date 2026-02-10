# Usamos una imagen de python completa en lugar de la slim para evitar falta de comandos
FROM python:3.10

# Evitar que Python genere archivos .pyc y asegurar salida de logs inmediata
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Instalar dependencias del sistema y el Driver de SQL Server
# Agregamos gnupg2 y ca-certificates por seguridad
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    ca-certificates \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev \
    && apt-get clean

# Directorio de trabajo
WORKDIR /app

# Copiar archivos del proyecto
COPY . /app/

# Instalar librerías de Python
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto de Render
EXPOSE 10000

# Ejecutar la aplicación
CMD ["python", "main.py"]