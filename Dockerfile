FROM python:3.10

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# 1. Instalamos dependencias básicas
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 2. Método moderno para agregar la llave de Microsoft (sin apt-key)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg

# 3. Registrar el repositorio de Microsoft usando la nueva llave
RUN curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list

# 4. Instalar el driver de SQL Server
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y \
    msodbcsql17 \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app/
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 10000

# Usamos python main.py para arrancar
CMD ["python", "main.py"]