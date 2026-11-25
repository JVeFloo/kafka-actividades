# Actividades de Kafka

Todas las actividades usan Kafka local (Docker, modo KRaft) en `localhost:9092` y Python con `kafka-python-ng`. Las Actividades 1 y 2 guardan datos en MongoDB (`localhost:27017`) y la Actividad 3 usa PySpark.

## Actividad 1: enriquecimiento de datos

- `productor_iot.py`: manda cada 2 segundos al topic `telemetria-iot` una lectura con `sensor_id` (de S-01 a S-04) y una `temperatura` aleatoria entre 15 y 38 ºC.
- `consumidor_iot.py`: lee el topic `telemetria-iot`, agrega `ubicacion`, `responsable` y `timestamp` según el sensor, y guarda el documento en la colección `telemetria_enriquecida` de MongoDB.

## Actividad 2: pipeline de dos topics

- `productor_sensor.py`: manda cada segundo al topic `telemetria-bruta` una lectura con `sensor_id`, `temperatura` (15 a 38 ºC) y `timestamp`.
- `filtro_alertas.py`: lee `telemetria-bruta`. Si la temperatura es mayor a 30 ºC la republica en el topic `alertas-criticas`, y si no, la descarta.
- `consumidor_mongodb.py`: lee solo `alertas-criticas` y guarda cada alerta en la colección `alertas_temperatura` de MongoDB.

## Actividad 3: Spark Structured Streaming

Leen el topic `alertas-criticas` y necesitan el pipeline de la Actividad 2 funcionando.

- `spark_conteo.py`: cuenta cuántas alertas ha generado cada sensor y muestra la tabla en consola cada 5 segundos.
- `spark_ventana.py`: calcula la temperatura media por sensor con una ventana de 30 segundos que avanza cada 10, usando el `timestamp` del mensaje como tiempo del evento y un watermark de 30 segundos.
