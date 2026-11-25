from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg
from pyspark.sql.functions import round as redondear
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Sesión de Spark con el conector de Kafka (versión igual a la de Spark)
spark = (SparkSession.builder
    .appName("VentanaTemperatura")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Estructura del JSON que manda el filtro
esquema = StructType([
    StructField("sensor_id", StringType()),
    StructField("temperatura", DoubleType()),
    StructField("timestamp", StringType())
])

# Lectura del topic alertas-criticas (solo alertas nuevas, en vivo)
lecturas = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "alertas-criticas")
    .option("startingOffsets", "latest")
    .load())

# Se pasa el value a texto, luego a columnas, y el timestamp del mensaje
# se convierte en tiempo del evento
alertas = (lecturas
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), esquema).alias("datos"))
    .select("datos.*")
    .withColumn("tiempo_evento", to_timestamp(col("timestamp"))))

# Temperatura media por sensor: ventana de 30 segundos que avanza cada 10
resultado = (alertas
    .withWatermark("tiempo_evento", "30 seconds")
    .groupBy(window(col("tiempo_evento"), "30 seconds", "10 seconds"), col("sensor_id"))
    .agg(redondear(avg("temperatura"), 2).alias("temp_media")))

# Se imprime en consola cada 5 segundos
consulta = (resultado.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="5 seconds")
    .start())

consulta.awaitTermination()
