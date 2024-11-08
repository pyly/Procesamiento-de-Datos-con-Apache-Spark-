# Importamos librerías necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('HotelReviewsAnalysis').getOrCreate()

# Define la ruta del archivo CSV
file_path = 'hdfs://localhost:9000/Tarea3/hotel_reviews.csv'

# Lee el archivo CSV
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Imprimir el esquema del DataFrame
print("Esquema del DataFrame:")
df.printSchema()

# Limpieza de datos
# Elimina filas con valores nulos en columnas clave
df = df.dropna(subset=['Rating(Out of 10)', 'Name', 'Review_Text', 'Area'])

# Filtra puntuaciones inválidas (asegurarse de que están entre 1 y 10)
df = df.filter((F.col('Rating(Out of 10)') >= 1) & (F.col('Rating(Out of 10)') <= 10))

# Muestra las primeras filas del DataFrame tras la limpieza
print("Muestra de datos limpios:")
df.show()

# Requerimiento 1: Análisis de distribución de puntuaciones por área
print("Distribución de las puntuaciones por área (orden descendente):\n")
area_rating_distribution = df.groupBy('Area').agg(F.avg('Rating(Out of 10)').alias('Average_Rating')).sort(F.col('Average_Rating').desc())
area_rating_distribution.show()

# Requerimiento 2: Cálculo de puntuación promedio por hotel
print("Puntuación promedio por hotel (orden descendente):\n")
average_rating_by_hotel = df.groupBy('Name').agg(F.avg('Rating(Out of 10)').alias('Average_Rating')).sort(F.col('Average_Rating').desc())
average_rating_by_hotel.show()

# Requerimiento 3: Análisis de palabras clave en reseñas
print("Palabras clave más comunes en las reseñas (top 10):\n")
# Convertir el texto a minúsculas para evitar duplicados por diferencias de mayúsculas y minúsculas
df = df.withColumn("Review_Text", F.lower(F.col("Review_Text")))
# Tokenizar y contar palabras
words_df = df.select(F.explode(F.split(F.col('Review_Text'), ' ')).alias('Word')) \
              .filter(F.length('Word') > 3) \
              .groupBy('Word').count().sort(F.col('count').desc())
words_df.show(10)

# Requerimiento 4: Tendencias mensuales de las puntuaciones
print("Tendencias mensuales de puntuaciones (orden cronológico):\n")
# Convertir la columna de fecha y extraer el año y el mes
df = df.withColumn('YearMonth', F.date_format(F.to_date(F.col('Review_Date'), 'MMM-yy'), 'yyyy-MM'))
monthly_trends = df.groupBy('YearMonth').agg(F.avg('Rating(Out of 10)').alias('Monthly_Avg_Rating')).sort('YearMonth')
monthly_trends.show()

# Requerimiento 5: Análisis de sentimiento (clasificación de reseñas)
print("Frecuencia de reseñas positivas y negativas (orden descendente):\n")
# Crear una columna de Sentimiento basado en el valor de Rating
df = df.withColumn('Sentiment', F.when(F.col('Rating(Out of 10)') >= 8, 'Positiva').otherwise('Negativa'))
sentiment_counts = df.groupBy('Sentiment').count().sort(F.col('count').desc())
sentiment_counts.show()
