from kafka import KafkaProducer
import json
import random
import time

# Conexión a Kafka. Los mensajes se mandan como JSON.
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

sensores = ["S-01", "S-02", "S-03", "S-04"]

print("Productor IoT iniciado. Enviando lecturas...")

while True:
    mensaje = {
        "sensor_id": random.choice(sensores),
        "temperatura": round(random.uniform(15, 38), 1)
    }
    producer.send('telemetria-iot', value=mensaje)
    producer.flush()
    print(f"Enviado: {mensaje}")
    time.sleep(2)
