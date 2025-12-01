FROM python:3.10-slim

WORKDIR /app

# Copier les fichiers de requirements et installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code du producer
COPY producer.py .

# Commande par défaut
CMD ["python", "producer.py"]
