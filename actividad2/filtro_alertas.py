from kafka import KafkaConsumer, KafkaProducer
import json

# Consumidor: lee las lecturas del topic de telemetría bruta
consumer = KafkaConsumer(
    'telemetria-bruta',
    bootstrap_servers=['localhost:9092'],
    group_id='grupo-filtro',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Productor: manda las alertas al topic de alertas críticas
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

print("Filtro de alertas iniciado. Esperando lecturas...")

for mensaje in consumer:
    lectura = mensaje.value

    if lectura["temperatura"] > 30:
        # Temperatura crítica: se republica en el otro topic
        producer.send('alertas-criticas', value=lectura)
        producer.flush()
        print(f"ALERTA enviada: {lectura}")
    else:
        # No supera el umbral: se descarta
        print(f"Descartada: {lectura}")
