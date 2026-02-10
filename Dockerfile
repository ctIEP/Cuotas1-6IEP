FROM python:3.10

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# 1. Instalamos dependencias básicas primero
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    ca-certificates \
    apt-utils \
    && rm -rf /var/lib/apt/lists/*

# 2. Descargamos la llave y configuramos el repositorio de Microsoft por separado
# Esto evita el error de "Failed writing body" al no usar pipes complejos
RUN curl https://packages.microsoft.com/keys/microsoft.asc > /tmp/microsoft.asc \
    && apt-key add /tmp/microsoft.asc \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

# 3. Instalamos el driver de SQL Server
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y \
    msodbcsql17 \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app/
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 10000

CMD ["python", "main.py"]