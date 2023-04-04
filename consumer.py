from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType , StructField, StructType
from pyspark.sql.functions import from_json, col


spark = SparkSession.builder.appName('ML-Projet').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPIC = 'stream'


SCHEMA = StructType([
    StructField("year", FloatType()),                 
    # StructField("month", FloatType()),          
    # StructField("user_account_id", FloatType()),             
    # StructField("user_lifetime", FloatType()),         
    # StructField("user_intake", FloatType()),                   
    # StructField("user_no_outgoing_activity_in_days", FloatType()),         
    # StructField("user_account_balance_last", FloatType()),     
    # StructField("user_spendings", FloatType()),    
    # StructField("user_has_outgoing_calls", FloatType()),               
    # StructField("user_has_outgoing_sms", FloatType()),         
    # StructField("user_use_gprs", FloatType()),              
    # StructField("user_does_reload", FloatType()),             
    # StructField("reloads_inactive_days", FloatType()),              
    # StructField("reloads_count", FloatType()),
    # StructField("reloads_sum", FloatType()),               
    # StructField("calls_outgoing_count", FloatType()),         
    # StructField("calls_outgoing_spendings", FloatType()),              
    # StructField("calls_outgoing_duration", FloatType()),             
    # StructField("calls_outgoing_spendings_max", FloatType()),              
    # StructField("calls_outgoing_duration_max", FloatType()),
    # StructField("calls_outgoing_inactive_days", FloatType()),               
    # StructField("calls_outgoing_to_onnet_count", FloatType()),         
    # StructField("calls_outgoing_to_onnet_spendings", FloatType()),              
    # StructField("calls_outgoing_to_onnet_duration", FloatType()),             
    # StructField("calls_outgoing_to_onnet_inactive_days", FloatType()),              
    # StructField("calls_outgoing_to_offnet_count", FloatType()),
    # StructField("calls_outgoing_to_offnet_spendings", FloatType()),               
    # StructField("calls_outgoing_to_offnet_duration", FloatType()),         
    # StructField("calls_outgoing_to_offnet_inactive_days", FloatType()),              
    # StructField("calls_outgoing_to_abroad_count", FloatType()),             
    # StructField("calls_outgoing_to_abroad_spendings", FloatType()),              
    # StructField("calls_outgoing_to_abroad_duration", FloatType())                 
])






df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load() \
    .select(from_json(col("value").cast("string"), SCHEMA).alias("json")) \
    .select("json.*")


df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()





