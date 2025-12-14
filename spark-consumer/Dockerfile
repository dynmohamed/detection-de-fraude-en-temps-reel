FROM jupyter/pyspark-notebook:latest

USER root

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Installer les dépendances système si nécessaire
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

# Copier les fichiers de requirements et installer les dépendances
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copier le code du consumer
COPY consumer.py .


# La commande sera définie dans docker-compose.yml
