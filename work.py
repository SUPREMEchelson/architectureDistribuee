from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Configurez un environnement PySpark
spark = SparkSession.builder \
    .appName("emploi") \
    .config("spark.jars", "/ArchitecDist_TP1/src/spark-sql-kafka-0-10_2.12-3.3.2.jar") \
    .getOrCreate()

# 2. Lisez les données depuis le topic Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "salarie") \
    .load()

# 3. Parsez et déserialisez les messages Kafka
schema = StructType([
    StructField("Surface", DoubleType(), True),
    StructField("Ville", StringType(), True),
    StructField("Code_postale", StringType(), True),
    StructField("Prix_par_mois", DoubleType(), True),
    StructField("intitule", StringType(), True),
    StructField("description", StringType(), True),
    StructField("salaire", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
])

json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# 4. Effectuez les opérations de transformation et d'agrégation
aggregated_df = json_df.groupBy("Code_postale", "intitule") \
    .agg(avg("salaire").alias("salaire_moyen"))

# 5. Écrivez les résultats dans une base de données MongoDB
mongo_uri = "mongodb://username:password@localhost:27017"
database_name = "my_database"
collection_name = "emploi_salaire"

aggregated_df.writeStream \
    .outputMode("update") \
    .format("mongo") \
    .option("uri", mongo_uri) \
    .option("database", database_name) \
    .option("collection", collection_name) \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start() \
    .awaitTermination()
