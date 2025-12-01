# Utiliser l'image PySpark qui inclut déjà Spark
FROM jupyter/pyspark-notebook:latest

USER root

# Installer les dépendances système nécessaires
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    netcat \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER jovyan

WORKDIR /home/jovyan/work

# Copier le fichier requirements
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Télécharger le connecteur PostgreSQL JDBC pour Spark
RUN mkdir -p /home/jovyan/.ivy2/jars && \
    curl -L https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
    -o /home/jovyan/.ivy2/jars/postgresql-42.7.1.jar

# Configuration Spark
ENV SPARK_HOME=/usr/local/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS='lab'

# Ports exposés
EXPOSE 8888 4040 4041

CMD ["start-notebook.sh", \
     "--ServerApp.token=''", \
     "--ServerApp.password=''", \
     "--ServerApp.allow_root=True", \
     "--ip=0.0.0.0"]