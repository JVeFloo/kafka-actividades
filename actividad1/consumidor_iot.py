from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import json

# Datos de cada sensor (los metadatos del Notion)
sensores_info = {
    "S-01": {"ubicacion": "Invernadero Norte", "responsable": "Ana"},
    "S-02": {"ubicacion": "Invernadero Sur", "responsable": "Pedro"},
    "S-03": {"ubicacion": "Almacén Semillas", "responsable": "Laura"},
    "S-04": {"ubicacion": "Zona de Cuarentena", "responsable": "Marc"},
}

# Conexión a MongoDB: base "invernadero", colección "telemetria_enriquecida"
cliente = MongoClient('mongodb://localhost:27017')
coleccion = cliente['invernadero']['telemetria_enriquecida']

# Conexión a Kafka: lee desde el inicio del topic
consumer = KafkaConsumer(
    'telemetria-iot',
    bootstrap_servers=['localhost:9092'],
    group_id='grupo-iot',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')))

print("Consumidor IoT iniciado. Esperando mensajes...")

for mensaje in consumer:
    lectura = mensaje.value
    info = sensores_info[lectura["sensor_id"]]

    # Se une la lectura con los datos del sensor
    documento = {
        "sensor_id": lectura["sensor_id"],
        "temperatura": lectura["temperatura"],
        "ubicacion": info["ubicacion"],
        "responsable": info["responsable"],
        "timestamp": datetime.now().isoformat()
    }

    print(f"Guardado: {documento}")
    coleccion.insert_one(documento)
