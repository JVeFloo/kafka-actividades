from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Conexión a MongoDB: base "invernadero", colección "alertas_temperatura"
cliente = MongoClient('mongodb://localhost:27017')
coleccion = cliente['invernadero']['alertas_temperatura']

# Conexión a Kafka: lee solo el topic de alertas críticas
consumer = KafkaConsumer(
    'alertas-criticas',
    bootstrap_servers=['localhost:9092'],
    group_id='grupo-mongodb',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')))

print("Consumidor MongoDB iniciado. Esperando alertas...")

for mensaje in consumer:
    alerta = mensaje.value
    print(f"Guardado: {alerta}")
    coleccion.insert_one(alerta)
