# --- Kafka Configuration ---
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "events"
CTR_RESULTS_TOPIC = "ctr_results"

# --- Generator Configuration ---
NUM_PARTITIONS = 3
WINDOW_SIZE_SEC = 10

# --- Spark Job Configuration ---
MAPPINGS_FILE = "mappings.json"
CHECKPOINT_DIR = "file:///tmp/spark-checkpoints" # Must be a file:// URI