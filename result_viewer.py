# result_viewer.py
import json
from datetime import datetime 
from confluent_kafka import Consumer
from config import BOOTSTRAP_SERVERS, CTR_RESULTS_TOPIC

def run_result_viewer():
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'result-viewer-group',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([CTR_RESULTS_TOPIC])

    print("Starting Result Viewer...")
    print(f"Listening for messages on topic '{CTR_RESULTS_TOPIC}'...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"Result viewer Kafka error: {msg.error()}")
                continue
            
            print("\n" + "="*50)
            print(f"Received result at {datetime.now().strftime('%H:%M:%S')}")
            print("="*50)
            
            payload = json.loads(msg.value().decode('utf-8'))
            print(json.dumps(payload, indent=2))

    except KeyboardInterrupt:
        print("\nShutting down result viewer...")
    finally:
        consumer.close()

if __name__ == '__main__':
    run_result_viewer()