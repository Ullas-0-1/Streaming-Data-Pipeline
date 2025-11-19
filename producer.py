from confluent_kafka import Producer
from multiprocessing import Process
import json
import time

from config import BOOTSTRAP_SERVERS, TOPIC_NAME, NUM_PARTITIONS


from generators import uniform, poisson, mmpp








NUM_PRODUCERS = 3
TOTAL_THROUGHPUT = 45000
TOTAL_SECONDS = 100
WINDOW_SIZE_SEC = 10
MAPPINGS_FILE = "mappings.json"


#Options: "UNIFORM", "POISSON", "MMPP"
GENERATOR_MODE = "POISSON"


#load mappings

def load_mappings_from_file(filename):
    """Loads the static ad_id -> campaign_id mappings from the shared file."""
    print(f"Loading ad mappings from '{filename}'...")
    try:
        with open(filename, 'r') as f:
            # The JSON file saves lists, convert them to tuples for random.choice
            mappings = [tuple(m) for m in json.load(f)]
        print(f"✅ Successfully loaded {len(mappings)} mappings.")
        return mappings
    except FileNotFoundError:
        print(f"🚨 ERROR: Mappings file '{filename}' not found.")
        print("Please run the 'populate_mappings.py' script first to generate it.")
        exit(1)
        
#create producer function
def create_producer():
    return Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "linger.ms": 2})

def producer_worker(producer_id, per_producer_rate, ad_mappings, generator_function):
    """
    A generic worker that runs the chosen generator function.
    """
    kafka_producer = create_producer()
    
    # Execute the selected generator function
    generator_function(
        kafka_producer=kafka_producer,
        topic_name=TOPIC_NAME,
        producer_id=producer_id,
        duration_sec=TOTAL_SECONDS,
        ad_mappings=ad_mappings,
        per_producer_rate=per_producer_rate,
        window_size_sec=WINDOW_SIZE_SEC
    )

def run_producers():
    if NUM_PRODUCERS != NUM_PARTITIONS:
        print("WARNING: NUM_PRODUCERS != NUM_PARTITIONS in config.")
    if NUM_PRODUCERS <= 0:
        raise ValueError("NUM_PRODUCERS must be >= 1")
    
    # Select the generator function based on the mode
    if GENERATOR_MODE == "UNIFORM":
        generator = uniform.generate
    elif GENERATOR_MODE == "POISSON":
        generator = poisson.generate
    elif GENERATOR_MODE == "MMPP":
        generator = mmpp.generate
    else:
        raise ValueError(f"Unknown GENERATOR_MODE: '{GENERATOR_MODE}'")

    ad_mappings = load_mappings_from_file(MAPPINGS_FILE)

    # Distribute the total throughput across all producers
    base_rate = TOTAL_THROUGHPUT // NUM_PRODUCERS
    remainder = TOTAL_THROUGHPUT % NUM_PRODUCERS
    per_producer_rates = [base_rate + (1 if i < remainder else 0) for i in range(NUM_PRODUCERS)]

    print("="*50)
    print(f"Starting producer with {GENERATOR_MODE} generator.")
    print(f"CONFIG -> NUM_PRODUCERS: {NUM_PRODUCERS}, TOTAL_THROUGHPUT: {TOTAL_THROUGHPUT}")
    print("="*50)

    procs = []
    for pid in range(NUM_PRODUCERS):
        p = Process(target=producer_worker, args=(pid, per_producer_rates[pid], ad_mappings, generator))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    print("All producers done.")

if __name__ == "__main__":
    run_producers()
