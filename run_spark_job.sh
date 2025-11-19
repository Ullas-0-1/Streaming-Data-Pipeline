#!/bin/zsh

# --- <<< MODIFIED SECTION >>> ---
# 1. We are now running this script with zsh (see the line above)
# 2. Explicitly source your zshrc file to load your exported
#    environment variables ($SPARK_HOME, $JAVA_17_HOME, etc.)
source ~/.zshrc
# --- <<< END OF MODIFIED SECTION >>> ---


# --- Configuration ---
# This package is correct for Spark 4.0.1 and Scala 2.13
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"

# --- End Configuration ---

echo "Submitting Spark job..."

# --- <<< MODIFIED COMMAND >>> ---
# Instead of using the alias "spark-submit17", we are now calling
# the full, direct command that your alias was a shortcut for.
# This is much more reliable inside a script.
JAVA_HOME=$JAVA_17_HOME $SPARK_HOME/bin/spark-submit \
  --master "local[4]" \
  --packages $KAFKA_PACKAGE \
  spark_ctr_job.py
# --- <<< END OF MODIFIED COMMAND >>> ---

echo "Spark job finished."