#!/bin/zsh

# 1. Source your .zshrc to load $JAVA_17_HOME and $SPARK_HOME
source ~/.zshrc

# --- Configuration ---
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"

# 2. Set the URL for your Spark Master.
# You will see this URL when you run `spark-master17`.
# It's almost always your laptop's name on port 7077.
SPARK_MASTER_URL="spark://Ullass-Laptop.local:7077"
# --- End Configuration ---

echo "Submitting Spark job to STANDALONE cluster at $SPARK_MASTER_URL..."

# 3. Submit the job
JAVA_HOME=$JAVA_17_HOME $SPARK_HOME/bin/spark-submit \
  --master $SPARK_MASTER_URL \
  --packages $KAFKA_PACKAGE \
  --conf spark.cores.max=4 \
  spark_ctr_job.py


echo "Spark job finished."