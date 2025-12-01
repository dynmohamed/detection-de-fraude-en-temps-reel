"""
Spark Streaming Consumer - Consommation depuis Kafka et transformation des données
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# Configuration
KAFKA_BROKER = "kafka:9093"
KAFKA_TOPIC = "data-stream"
OUTPUT_PATH = "/data/processed"

def create_spark_session():
    """Crée et retourne une session Spark avec les packages Kafka"""
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✓ Session Spark créée avec succès")
    return spark

def process_stream(spark):
    """Lit le stream Kafka et applique des transformations"""
    
    # Lecture du stream depuis Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("kafka.session.timeout.ms", "30000") \
        .option("kafka.request.timeout.ms", "120000") \
        .option("kafka.default.api.timeout.ms", "120000") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print(f"✓ Connexion au topic Kafka '{KAFKA_TOPIC}' établie")
    
    # Convertir les données binaires en string
    df_string = df.selectExpr("CAST(value AS STRING) as json_data", "timestamp")
    
    # Parser le JSON (vous pouvez adapter le schéma selon vos données)
    # Comme on ne connaît pas la structure exacte, on garde le JSON en string
    # et on ajoute des colonnes de traitement
    
    df_transformed = df_string \
        .withColumn("processing_time", current_timestamp()) \
        .withColumn("kafka_timestamp", col("timestamp"))
    
    # Afficher les données dans la console uniquement
    query = df_transformed \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') \
        .start()
    
    print("✓ Streaming vers la console démarré")
    
    return query

def main():
    """Fonction principale"""
    print("=" * 60)
    print("SPARK STREAMING CONSUMER - Traitement des données Kafka")
    print("=" * 60)
    
    try:
        # Créer la session Spark
        spark = create_spark_session()
        
        # Démarrer le traitement du stream
        query = process_stream(spark)
        
        print("\n→ Streaming en cours... (Ctrl+C pour arrêter)")
        print("-" * 60)
        
        # Attendre la fin du streaming
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n\n✓ Arrêt du streaming demandé")
    except Exception as e:
        print(f"\n✗ Erreur: {e}")
    finally:
        print("✓ Consumer Spark arrêté")

if __name__ == "__main__":
    main()
