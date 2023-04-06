import pandas as pd

data = pd.read_csv('/ArchitecDist_TP1/data/emploi.csv')

from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Changez cette ligne pour utiliser l'adresse correcte
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'salarie'

for index, row in data.iterrows():
    # Convertir la ligne en dictionnaire
    row_dict = row.to_dict()
    
    # Envoyer la ligne au topic Kafka
    producer.send(topic, row_dict)

# Attendre que toutes les messages soient envoyés
producer.flush()


