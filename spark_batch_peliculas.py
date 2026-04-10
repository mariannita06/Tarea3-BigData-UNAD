from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as _sum, desc, round as _round, when, isnull
from pyspark.sql.types import DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("Tarea3_Peliculas") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ SparkSession iniciada")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .option("sep", ";") \
.csv("peliculas.csv")

print(f"Total peliculas: {df.count()}")
df.printSchema()
df.show(5, truncate=True)

df_clean = df \
    .filter(col("title").isNotNull()) \
    .filter(col("rating").isNotNull()) \
    .withColumn("rating", col("rating").cast(DoubleType())) \
    .withColumn("vote_count", col("vote_count").cast(IntegerType())) \
    .filter(col("rating") > 0) \
    .filter(col("vote_count") > 10)

print(f"Peliculas despues de limpieza: {df_clean.count()}")

rdd = df_clean.rdd
rdd_genre = rdd.map(lambda r: (r["genre"] if r["genre"] else "Sin genero", float(r["rating"])))
rdd_grouped = rdd_genre.groupByKey()
rdd_avg = rdd_grouped.map(lambda x: (x[0], round(sum(x[1])/len(list(x[1])),2))).sortBy(lambda x: -x[1])

print("\nTop generos por rating (RDD):")
for g, r in rdd_avg.take(5):
    print(f"  {g}: {r}")

print("\nTop 10 peliculas mejor calificadas:")
df_clean.filter(col("vote_count") >= 100) \
    .select("title", "genre", "rating", "vote_count", "director") \
    .orderBy(desc("rating")) \
    .show(10, truncate=False)

print("\nGeneros mas populares:")
df_clean.groupBy("genre") \
    .agg(count("title").alias("cantidad"), _round(avg("rating"),2).alias("rating_promedio")) \
    .orderBy(desc("cantidad")) \
    .show(10)

print("\nTop directores:")
df_clean.filter(col("director").isNotNull()) \
    .groupBy("director") \
    .agg(count("title").alias("peliculas"), _round(avg("rating"),2).alias("rating_promedio")) \
    .orderBy(desc("peliculas")) \
    .show(10, truncate=False)

print("\n✅ Analisis completado!")
spark.stop()
