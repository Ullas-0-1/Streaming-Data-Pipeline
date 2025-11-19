#!/bin/zsh

source ~/.zshrc

# --- CRITICAL FIX: FORCE JAVA 17 ---
# We explicitly set JAVA_HOME to the Java 17 path defined in your .zshrc.
# This overrides the system default (Java 24) which causes the Security Manager error.
export JAVA_HOME=$JAVA_17_HOME
export PATH=$JAVA_HOME/bin:$PATH

# --- EXECUTION ---
echo "Submitting Flink Job to Standalone Cluster..."
echo "Using Java Home: $JAVA_HOME"

# Clear security flags that might cause Java 17 startup issues
export JAVA_TOOL_OPTIONS=""
export FLINK_ENV_JAVA_OPTS=""

# Since jars are in $FLINK_HOME/lib, we don't need -j.
# We removed -pyclient as requested.
# We use absolute path for the python script to be safe.
$FLINK_HOME/bin/flink run \
  -py $PWD/flink_ctr_job.py
  
echo "Flink job submitted successfully."