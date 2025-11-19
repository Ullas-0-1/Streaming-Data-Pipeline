
# import json
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType, DoubleType
# # <<< MODIFIED: We need date_format and to_utc_timestamp >>>
# from pyspark.sql.functions import col, from_json, window, count, when, lit, coalesce, current_timestamp, to_utc_timestamp, date_format, max as spark_max, to_json, struct, collect_list

# from config import BOOTSTRAP_SERVERS, TOPIC_NAME, CTR_RESULTS_TOPIC, MAPPINGS_FILE, WINDOW_SIZE_SEC, CHECKPOINT_DIR

# def run_spark_streaming_job():
    
#     # 1. Define the schema for the incoming Kafka JSON data
#     event_schema = StructType([
#         StructField("user_id", StringType(), False),
#         StructField("page_id", StringType(), False),
#         StructField("ad_id", StringType(), False),
#         StructField("ad_type", StringType(), True),
#         StructField("event_type", StringType(), True),
#         StructField("event_time_ns", LongType(), False),
#         StructField("ip_address", StringType(), True),
#         StructField("campaign_id", StringType(), True), 
#         StructField("window_id", IntegerType(), True)
#     ])

#     # 2. Initialize the SparkSession
#     spark = SparkSession.builder \
#         .appName("RealTimeCTRAnalytics") \
#         .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
#         .config("spark.sql.session.timeZone", "UTC") \
#         .getOrCreate()
    
#     spark.sparkContext.setLogLevel("WARN")
#     print("SparkSession Initialized. Reading static mappings...")

#     # 3. Load the static ad_id -> campaign_id mappings
#     try:
#         with open(MAPPINGS_FILE, 'r') as f:
#             mappings_list = json.load(f)
        
#         mappings_df = spark.createDataFrame(mappings_list, ["ad_id", "campaign_id"])
#         mappings_df.cache()
        
#         print(f"Successfully loaded {mappings_df.count()} mappings into Spark DataFrame.")

#     except Exception as e:
#         print(f"CRITICAL ERROR: Could not load or create mappings_df from {MAPPINGS_FILE}.")
#         print(f"Error: {e}")
#         spark.stop()
#         return

#     print("Static mappings loaded successfully. Defining Kafka source...")

#     # 4. Define the Kafka Source (replaces consumer.py)
#     kafka_stream_df = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
#         .option("subscribe", TOPIC_NAME) \
#         .option("startingOffsets", "earliest") \
#         .load()

#     # 5. Parse the Kafka data
#     parsed_stream_df = kafka_stream_df \
#         .select(
#             col("value").cast("string").alias("json"),
#             col("timestamp").alias("kafka_produce_time") # This is our 'latest_produce_time'
#         ) \
#         .withColumn("data", from_json(col("json"), event_schema)) \
#         .select("data.*", "kafka_produce_time")

#     # 6. Transform data: Convert nanoseconds to a proper Spark Timestamp
#     transformed_df = parsed_stream_df \
#         .withColumn("event_timestamp", (col("event_time_ns") / 1_000_000_000).cast("timestamp")) \
#         .drop("campaign_id") 

#     # 7. Join with static mappings to get campaign_id (replaces SQL JOIN)
#     joined_df = transformed_df \
#         .join(mappings_df, "ad_id", "left_outer")
    
#     # 8. Define Watermark and Window (replaces your aggregator.py logic)
#     watermarked_df = joined_df \
#         .withWatermark("event_timestamp", "1 seconds") \
#         .groupBy(
#             window(col("event_timestamp"), f"{WINDOW_SIZE_SEC} seconds").alias("window"),
#             col("campaign_id")
#         )

#     # 9. Perform the CTR Aggregation (replaces your SQL query)
#     ctr_df = watermarked_df.agg(
#         count(when(col("event_type") == "view", 1)).alias("view_count"),
#         count(when(col("event_type") == "click", 1)).alias("click_count"),
#         spark_max("kafka_produce_time").alias("latest_produce_time")
#     ).withColumn(
#         "ctr",
#         coalesce(col("click_count"), lit(0)) / coalesce(col("view_count"), lit(1))
#     )

#     # 10. Format the output to match what latency_analyzer.py expects
#     # <<< THIS IS THE DEFINITIVE FIX >>>
#     output_df = ctr_df \
#         .withColumn("window_id", (col("window.start").cast("long") / WINDOW_SIZE_SEC).cast("integer")) \
#         .withColumn(
#             "latest_insert_time",
#             # Get current UTC time and format it as '2025-09-30T10:30:00.123Z'
#             date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
#         ) \
#         .withColumn(
#             "latest_produce_time",
#             # Format the Kafka timestamp to the same 'Z' (UTC) format
#             date_format(col("latest_produce_time").cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
#         ) \
#         .select(
#             col("window_id"),
#             col("campaign_id"),
#             col("view_count"),
#             col("click_count"),
#             col("ctr"),
#             col("latest_produce_time"),
#             col("latest_insert_time")
#         )
    
#     # 11. Re-structure to match the *exact* final JSON format.
#     final_output_df = output_df \
#         .groupBy("window_id", "latest_produce_time", "latest_insert_time") \
#         .agg(
#             collect_list(
#                 struct(col("campaign_id"), col("view_count"), col("click_count"), col("ctr"))
#             ).alias("ctr_results")
#         ) \
#         .select(
#             to_json(
#                 struct(col("window_id"), col("latest_produce_time"), col("latest_insert_time"), col("ctr_results"))
#             ).alias("value")
#         )

#     # 12. Define the Kafka Sink (replaces producer logic in aggregator.py)
#     query = final_output_df.writeStream \
#         .outputMode("update") \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
#         .option("topic", CTR_RESULTS_TOPIC) \
#         .option("checkpointLocation", CHECKPOINT_DIR) \
#         .start()

#     print(f"Spark job started. Writing aggregated results to '{CTR_RESULTS_TOPIC}'.")
#     print(f"Using checkpoint location: '{CHECKPOINT_DIR}'")
    
#     query.awaitTermination()

# if __name__ == "__main__":
#     run_spark_streaming_job()












#above one is the 0 second trigger mode while below is 5 second trigger mode -- becasue of which the writes to the sink will be batched every 5 seconds



import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType, DoubleType
# <<< MODIFIED: We need date_format and to_utc_timestamp >>>
from pyspark.sql.functions import col, from_json, window, count, when, lit, coalesce, current_timestamp, to_utc_timestamp, date_format, max as spark_max, to_json, struct, collect_list

from config import BOOTSTRAP_SERVERS, TOPIC_NAME, CTR_RESULTS_TOPIC, MAPPINGS_FILE, WINDOW_SIZE_SEC, CHECKPOINT_DIR

def run_spark_streaming_job():
    
    # 1. Define the schema for the incoming Kafka JSON data
    event_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("page_id", StringType(), False),
        StructField("ad_id", StringType(), False),
        StructField("ad_type", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time_ns", LongType(), False),
        StructField("ip_address", StringType(), True),
        StructField("campaign_id", StringType(), True), 
        StructField("window_id", IntegerType(), True)
    ])

    # 2. Initialize the SparkSession
    spark = (
        SparkSession.builder
        .appName("RealTimeCTRAnalytics")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession Initialized. Reading static mappings...")

    # 3. Load the static ad_id -> campaign_id mappings
    try:
        with open(MAPPINGS_FILE, 'r') as f:
            mappings_list = json.load(f)
        
        mappings_df = spark.createDataFrame(mappings_list, ["ad_id", "campaign_id"])
        mappings_df.cache()
        
        print(f"Successfully loaded {mappings_df.count()} mappings into Spark DataFrame.")

    except Exception as e:
        print(f"CRITICAL ERROR: Could not load or create mappings_df from {MAPPINGS_FILE}.")
        print(f"Error: {e}")
        spark.stop()
        return

    print("Static mappings loaded successfully. Defining Kafka source...")

    # 4. Define the Kafka Source (replaces consumer.py)
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    # 5. Parse the Kafka data
    parsed_stream_df = kafka_stream_df \
        .select(
            col("value").cast("string").alias("json"),
            col("timestamp").alias("kafka_produce_time") # This is our 'latest_produce_time'
        ) \
        .withColumn("data", from_json(col("json"), event_schema)) \
        .select("data.*", "kafka_produce_time")

    # 6. Transform data: Convert nanoseconds to a proper Spark Timestamp
    transformed_df = parsed_stream_df \
        .withColumn("event_timestamp", (col("event_time_ns") / 1_000_000_000).cast("timestamp")) \
        .drop("campaign_id") 

    # 7. Join with static mappings to get campaign_id (replaces SQL JOIN)
    joined_df = transformed_df \
        .join(mappings_df, "ad_id", "left_outer")
    
    # 8. Define Watermark and Window (replaces your aggregator.py logic)
    watermarked_df = joined_df \
        .withWatermark("event_timestamp", "10 seconds") \
        .groupBy(
            window(col("event_timestamp"), f"{WINDOW_SIZE_SEC} seconds").alias("window"),
            col("campaign_id")
        )

    # 9. Perform the CTR Aggregation (replaces your SQL query)
    ctr_df = watermarked_df.agg(
        count(when(col("event_type") == "view", 1)).alias("view_count"),
        count(when(col("event_type") == "click", 1)).alias("click_count"),
        spark_max("kafka_produce_time").alias("latest_produce_time")
    ).withColumn(
        "ctr",
        coalesce(col("click_count"), lit(0)) / coalesce(col("view_count"), lit(1))
    )

    # 10. Format the output to match what latency_analyzer.py expects
    output_df = ctr_df \
        .withColumn("window_id", (col("window.start").cast("long") / WINDOW_SIZE_SEC).cast("integer")) \
        .withColumn(
            "latest_insert_time",
            date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        ) \
        .withColumn(
            "latest_produce_time",
            date_format(col("latest_produce_time").cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        ) \
        .select(
            col("window_id"),
            col("campaign_id"),
            col("view_count"),
            col("click_count"),
            col("ctr"),
            col("latest_produce_time"),
            col("latest_insert_time")
        )
    
    # 11. Re-structure to match the *exact* final JSON format.
    final_output_df = output_df \
        .groupBy("window_id", "latest_produce_time", "latest_insert_time") \
        .agg(
            collect_list(
                struct(col("campaign_id"), col("view_count"), col("click_count"), col("ctr"))
            ).alias("ctr_results")
        ) \
        .select(
            to_json(
                struct(col("window_id"), col("latest_produce_time"), col("latest_insert_time"), col("ctr_results"))
            ).alias("value")
        )

    # 12. Define the Kafka Sink (replaces producer logic in aggregator.py)
    # <<< NEW: Add a trigger to batch results every 5 seconds >>>
    query = final_output_df.writeStream \
        .outputMode("update") \
        .trigger(processingTime="5 seconds") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("topic", CTR_RESULTS_TOPIC) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .start()

    print(f"Spark job started. Writing aggregated results to '{CTR_RESULTS_TOPIC}'.")
    print(f"Using checkpoint location: '{CHECKPOINT_DIR}'")
    
    query.awaitTermination()

if __name__ == "__main__":
    run_spark_streaming_job()










######APPEND MODE __ FOR EXEPRIMENTATION PURPOSES ONLY -- ###########
# import json
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
# from pyspark.sql.functions import col, from_json, window, count, when, lit, coalesce, current_timestamp, to_utc_timestamp, date_format, max as spark_max, to_json, struct, collect_list

# from config import BOOTSTRAP_SERVERS, TOPIC_NAME, CTR_RESULTS_TOPIC, MAPPINGS_FILE, WINDOW_SIZE_SEC, CHECKPOINT_DIR

# def run_spark_streaming_job():
    
#     event_schema = StructType([
#         StructField("user_id", StringType(), False),
#         StructField("page_id", StringType(), False),
#         StructField("ad_id", StringType(), False),
#         StructField("ad_type", StringType(), True),
#         StructField("event_type", StringType(), True),
#         StructField("event_time_ns", LongType(), False),
#         StructField("ip_address", StringType(), True),
#         StructField("campaign_id", StringType(), True), 
#         StructField("window_id", IntegerType(), True)
#     ])

#     # Initialize Spark Session
#     spark = SparkSession.builder \
#         .appName("RealTimeCTRAnalytics_AppendMode") \
#         .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
#         .config("spark.sql.session.timeZone", "UTC") \
#         .config("spark.sql.shuffle.partitions", "4") \
#         .getOrCreate()
    
#     spark.sparkContext.setLogLevel("WARN")
#     print("SparkSession Initialized. Reading static mappings...")

#     # Load Mappings
#     try:
#         with open(MAPPINGS_FILE, 'r') as f:
#             mappings_list = json.load(f)
#         mappings_df = spark.createDataFrame(mappings_list, ["ad_id", "campaign_id"])
#         mappings_df.cache()
#         print(f"Successfully loaded {mappings_df.count()} mappings into Spark DataFrame.")
#     except Exception as e:
#         print(f"CRITICAL ERROR: Could not load or create mappings_df from {MAPPINGS_FILE}.")
#         print(f"Error: {e}")
#         spark.stop()
#         return

#     print("Static mappings loaded successfully. Defining Kafka source...")

#     # Read from Kafka
#     kafka_stream_df = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
#         .option("subscribe", TOPIC_NAME) \
#         .option("startingOffsets", "earliest") \
#         .load()

#     # Parse JSON
#     parsed_stream_df = kafka_stream_df \
#         .select(
#             col("value").cast("string").alias("json"),
#             col("timestamp").alias("kafka_produce_time")
#         ) \
#         .withColumn("data", from_json(col("json"), event_schema)) \
#         .select("data.*", "kafka_produce_time")

#     # Transform Timestamp
#     transformed_df = parsed_stream_df \
#         .withColumn("event_timestamp", (col("event_time_ns") / 1_000_000_000).cast("timestamp")) \
#         .drop("campaign_id") 

#     # Join
#     joined_df = transformed_df \
#         .join(mappings_df, "ad_id", "left_outer")
    
#     # Watermark & Window
#     # Reduced to 1 second so Append mode emits results relatively quickly
#     watermarked_df = joined_df \
#         .withWatermark("event_timestamp", "5 second") \
#         .groupBy(
#             window(col("event_timestamp"), f"{WINDOW_SIZE_SEC} seconds").alias("window"),
#             col("campaign_id")
#         )

#     # Aggregation 1: Calculate CTR
#     ctr_df = watermarked_df.agg(
#         count(when(col("event_type") == "view", 1)).alias("view_count"),
#         count(when(col("event_type") == "click", 1)).alias("click_count"),
#         spark_max("kafka_produce_time").alias("latest_produce_time")
#     ).withColumn(
#         "ctr",
#         coalesce(col("click_count"), lit(0)) / coalesce(col("view_count"), lit(1))
#     )

#     # Prepare Output
#     output_df = ctr_df \
#         .withColumn("window_id", (col("window.start").cast("long") / WINDOW_SIZE_SEC).cast("integer")) \
#         .withColumn(
#             "latest_insert_time",
#             date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
#         ) \
#         .withColumn(
#             "latest_produce_time",
#             date_format(col("latest_produce_time").cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
#         ) \
#         .select(
#             # <<< CRITICAL FIX FOR APPEND MODE: Keep the 'window' column >>>
#             col("window"),
#             col("window_id"),
#             col("campaign_id"),
#             col("view_count"),
#             col("click_count"),
#             col("ctr"),
#             col("latest_produce_time"),
#             col("latest_insert_time")
#         )
    
#     # Aggregation 2: Group into List
#     # <<< CRITICAL FIX: Group by 'window' to maintain watermark context >>>
#     final_output_df = output_df \
#         .groupBy("window", "window_id", "latest_produce_time", "latest_insert_time") \
#         .agg(
#             collect_list(
#                 struct(col("campaign_id"), col("view_count"), col("click_count"), col("ctr"))
#             ).alias("ctr_results")
#         ) \
#         .select(
#             to_json(
#                 struct(col("window_id"), col("latest_produce_time"), col("latest_insert_time"), col("ctr_results"))
#             ).alias("value")
#         )

#     # Write to Kafka in APPEND Mode
#     query = final_output_df.writeStream \
#         .outputMode("append") \
#         .trigger(processingTime="5 seconds") \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
#         .option("topic", CTR_RESULTS_TOPIC) \
#         .option("checkpointLocation", CHECKPOINT_DIR) \
#         .start()

#     print(f"Spark job started. Writing aggregated results to '{CTR_RESULTS_TOPIC}'.")
#     print(f"Using checkpoint location: '{CHECKPOINT_DIR}'")
    
#     query.awaitTermination()

# if __name__ == "__main__":
#     run_spark_streaming_job()