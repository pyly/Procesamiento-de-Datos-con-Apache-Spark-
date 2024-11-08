# urban_data_consumer.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, when, round, avg
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, TimestampType

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("UrbanDataAnalysisWithAlerts") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Esquema de los datos recibidos
schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("noise_level", FloatType(), True),
    StructField("co2_level", FloatType(), True)
])

# Leer datos de Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "urban_data") \
    .load()

# Parsear el JSON desde el valor recibido en Kafka
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Aplicar transformación y calcular promedios y alertas
windowed_stats = parsed_df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window(col("timestamp"), "1 minute"), "location_id") \
    .agg(
        round(avg("noise_level"), 2).alias("avg_noise_level"),
        round(avg("co2_level"), 2).alias("avg_co2_level")
    ) \
    .withColumn(
        "alert_noise_level", 
        when((col("avg_noise_level") > 85), "Noise Alert").otherwise("")
    ) \
    .withColumn(
        "alert_co2_level", 
        when((col("avg_co2_level") > 500), "CO2 Alert").otherwise("")
    )

# Mostrar resultados en la consola
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

