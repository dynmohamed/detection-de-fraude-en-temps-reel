"""
Spark Streaming Consumer - Consommation depuis Kafka, Prédiction et Stockage PostgreSQL
"""
import sys
import json
import joblib
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Configuration
KAFKA_BROKER = "kafka:9093"
KAFKA_TOPIC = "data-stream"
MODEL_PATH = "/app/model_finale"
POSTGRES_HOST = "postgres"
POSTGRES_DB = "frauddb"
POSTGRES_USER = "myuser"
POSTGRES_PASSWORD = "mypassword"

# Charger les modèles (Globalement sur le driver)
try:
    print("Chargement des modèles...")
    model = joblib.load(f"{MODEL_PATH}/LightGBM.pkl")
    print("Modèles chargés avec succès")
except Exception as e:
    print(f"Erreur lors du chargement des modèles: {e}")

def create_spark_session():
    """Crée et retourne une session Spark avec les packages Kafka"""
    spark = SparkSession.builder \
        .appName("FraudDetectionConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def save_to_postgres(df_pandas):
    """Sauvegarde les résultats dans PostgreSQL"""
    if df_pandas.empty:
        return

    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS fraud_predictions_new (
            transaction_id SERIAL PRIMARY KEY,
            step INT,
            type VARCHAR(50),
            amount FLOAT,
            oldbalanceOrg FLOAT,
            newbalanceOrig FLOAT,
            oldbalanceDest FLOAT,
            newbalanceDest FLOAT,
            prediction INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create_table_query)
        
        insert_query = """
        INSERT INTO fraud_predictions_new (step, type, amount, oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest, prediction)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for i, row in df_pandas.iterrows():
            try:
                cur.execute(insert_query, (
                    int(row['step']), 
                    str(row['type']), 
                    float(row['amount']), 
                    float(row['oldbalanceorg']), 
                    float(row['newbalanceorig']), 
                    float(row['oldbalancedest']), 
                    float(row['newbalancedest']), 
                    int(row['prediction'])
                ))
            except Exception as e:
                print(f"Erreur insertion ligne {i}: {e}")
                conn.rollback() 
                break
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"{len(df_pandas)} prédictions sauvegardées dans PostgreSQL")
        
    except Exception as e:
        print(f"Erreur PostgreSQL: {e}")

def process_batch(batch_df, batch_id):
    """Fonction appliquée à chaque micro-batch"""
    print(f"Traitement du batch {batch_id}...")
    
    count = batch_df.count()
    print(f"Batch {batch_id}: {count} enregistrements")
    
    if count == 0:
        return
        
    print(f"Batch {batch_id}: Conversion en Pandas...")
    pdf = batch_df.toPandas()
    print(f"Batch {batch_id}: Conversion terminée")
    
    pdf.columns = [c.lower() for c in pdf.columns]
    
    data_for_pred = pdf.copy()
    
    cols_to_drop = ['nameorig', 'namedest']
    data_for_pred = data_for_pred.drop(columns=[c for c in cols_to_drop if c in data_for_pred.columns])
    
    try:
        # Manual Preprocessing to match LightGBM model (7 features, Label Encoded type)
        type_mapping = {
            'CASH_IN': 0,
            'CASH_OUT': 1,
            'DEBIT': 2,
            'PAYMENT': 3,
            'TRANSFER': 4
        }
        
        if 'type' in data_for_pred.columns:
            data_for_pred['type'] = data_for_pred['type'].str.upper().map(type_mapping)
            data_for_pred['type'] = data_for_pred['type'].fillna(-1).astype(int)
        
        # Ensure column order matches training
        feature_cols = ['step', 'type', 'amount', 'oldbalanceorg', 'newbalanceorig', 'oldbalancedest', 'newbalancedest']
        
        missing_cols = [c for c in feature_cols if c not in data_for_pred.columns]
        if missing_cols:
            raise ValueError(f"Missing columns for prediction: {missing_cols}")
            
        X_processed = data_for_pred[feature_cols]
        
        print(f"Batch {batch_id}: Prédiction...")
        predictions = model.predict(X_processed)
        print(f"Batch {batch_id}: Prédiction terminée")
        
        pdf['prediction'] = predictions
        
        print(f"Batch {batch_id}: Sauvegarde dans Postgres...")
        save_to_postgres(pdf)
        print(f"Batch {batch_id}: Sauvegarde terminée")
        
    except Exception as e:
        print(f"Erreur lors du traitement/prédiction: {e}")

def main():
    spark = create_spark_session()
    
    # Définition du schéma des données entrantes (tout en String pour gérer le JSON)
    schema = StructType([
        StructField("step", StringType()),
        StructField("type", StringType()),
        StructField("amount", StringType()),
        StructField("nameOrig", StringType()),
        StructField("oldbalanceOrg", StringType()),
        StructField("newbalanceOrig", StringType()),
        StructField("nameDest", StringType()),
        StructField("oldbalanceDest", StringType()),
        StructField("newbalanceDest", StringType())
    ])
    
    # Lecture du stream Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()
    
    # Parsing du JSON et casting explicite
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    parsed_df = parsed_df \
        .withColumn("step", col("step").cast(IntegerType())) \
        .withColumn("amount", col("amount").cast(DoubleType())) \
        .withColumn("oldbalanceOrg", col("oldbalanceOrg").cast(DoubleType())) \
        .withColumn("newbalanceOrig", col("newbalanceOrig").cast(DoubleType())) \
        .withColumn("oldbalanceDest", col("oldbalanceDest").cast(DoubleType())) \
        .withColumn("newbalanceDest", col("newbalanceDest").cast(DoubleType()))
    
    # Démarrage du streaming avec foreachBatch
    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
