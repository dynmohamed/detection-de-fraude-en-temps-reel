"""
Kafka Producer - Lecture du fichier CSV et envoi des données vers Kafka
"""
import csv
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os

# Configuration
KAFKA_BROKER = 'kafka:9093'
KAFKA_TOPIC = 'data-stream'
CSV_FILE_PATH = '/data/new_data.csv'

def create_producer():
    """Crée et retourne un producer Kafka"""
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            print(f"✓ Connexion au broker Kafka réussie: {KAFKA_BROKER}")
            return producer
        except Exception as e:
            retry_count += 1
            print(f"⏳ Tentative {retry_count}/{max_retries} - Attente de Kafka...")
            time.sleep(5)
    
    print(f"✗ Impossible de se connecter à Kafka après {max_retries} tentatives")
    return None

def read_and_stream_csv(producer):
    """Lit le fichier CSV et envoie chaque ligne vers Kafka"""
    if not os.path.exists(CSV_FILE_PATH):
        print(f"✗ Fichier {CSV_FILE_PATH} introuvable!")
        print(f"  Veuillez placer votre fichier data_str.csv dans le dossier 'data'")
        return
    
    try:
        with open(CSV_FILE_PATH, 'r', encoding='utf-8') as csvfile:
            # Detecter le delimiteur automatiquement ou utiliser ; par defaut
            sample = csvfile.read(1024)
            csvfile.seek(0)
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
                delimiter = dialect.delimiter
            except:
                delimiter = ';' # Fallback
            
            print(f"✓ Utilisation du délimiteur: '{delimiter}'")
            csv_reader = csv.DictReader(csvfile, delimiter=delimiter)

            
            print(f"\n✓ Lecture du fichier: {CSV_FILE_PATH}")
            print(f"✓ Colonnes détectées: {csv_reader.fieldnames}\n")
            print(f"→ Début du streaming vers le topic '{KAFKA_TOPIC}'...\n")
            
            count = 0
            for row in csv_reader:
                try:
                    # Envoi du message vers Kafka
                    future = producer.send(KAFKA_TOPIC, value=row)
                    
                    # Attendre la confirmation (optionnel)
                    record_metadata = future.get(timeout=10)
                    
                    count += 1
                    # Afficher chaque ligne envoyée
                    print(f"  → [{count}] {row}")
                    
                    # Pas de pause pour envoyer rapidement
                    
                except KafkaError as e:
                    print(f"✗ Erreur d'envoi: {e}")
            
            print(f"\n✓ Streaming terminé: {count} enregistrements envoyés avec succès!")
            
    except FileNotFoundError:
        print(f"✗ Fichier {CSV_FILE_PATH} introuvable")
    except Exception as e:
        print(f"✗ Erreur lors de la lecture du CSV: {e}")

def main():
    """Fonction principale"""
    print("=" * 60)
    print("KAFKA PRODUCER - Streaming de données CSV")
    print("=" * 60)
    
    # Créer le producer
    producer = create_producer()
    if not producer:
        return
    
    try:
        # Lire et streamer les données
        read_and_stream_csv(producer)
    finally:
        # Fermer proprement le producer
        producer.flush()
        producer.close()
        print("\n✓ Producer Kafka fermé")

if __name__ == "__main__":
    main()
