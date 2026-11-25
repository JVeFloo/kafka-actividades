from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Sesión de Spark con el conector de Kafka (versión igual a la de Spark)
spark = (SparkSession.builder
    .appName("ConteoAlertas")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Estructura del JSON que manda el filtro
esquema = StructType([
    StructField("sensor_id", StringType()),
    StructField("temperatura", DoubleType()),
    StructField("timestamp", StringType())
])

# Lectura del topic alertas-criticas
lecturas = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "alertas-criticas")
    .option("startingOffsets", "earliest")
    .load())

# El campo value llega en binario: se pasa a texto y luego a columnas
alertas = (lecturas
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), esquema).alias("datos"))
    .select("datos.*"))

# Cuántas alertas ha generado cada sensor
conteo = alertas.groupBy("sensor_id").count()

# Se imprime la tabla en consola cada 5 segundos
consulta = (conteo.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="5 seconds")
    .start())

consulta.awaitTermination()
