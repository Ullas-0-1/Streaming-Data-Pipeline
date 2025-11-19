

# latency_analyzer.py
import json
from datetime import datetime, timezone
from confluent_kafka import Consumer
from config import BOOTSTRAP_SERVERS, CTR_RESULTS_TOPIC

def run_latency_analyzer():
    conf = {'bootstrap.servers': BOOTSTRAP_SERVERS, 'group.id': 'latency-analyzer-group', 'auto.offset.reset': 'latest'}
    consumer = Consumer(conf)
    consumer.subscribe([CTR_RESULTS_TOPIC])

    print("--- Starting Latency Analyzer ---")
    print("Waiting for results from the aggregator...")
    
    ingestion_latencies = []
    aggregation_latencies = []
    total_latencies = []
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                print(f"Latency analyzer Kafka error: {msg.error()}"); continue
            
            # This is the "Result Generation Time" (T3)
            result_generation_ms = msg.timestamp()[1]
            result_generation_time = datetime.fromtimestamp(result_generation_ms / 1000, tz=timezone.utc)
            
            payload = json.loads(msg.value().decode('utf-8'))
            window_id = payload['window_id']
            
            latest_produce_str = payload.get('latest_produce_time')
            latest_insert_str = payload.get('latest_insert_time')
            
            print(f"\n--- Results for Window {window_id} ---")

            if latest_produce_str and latest_insert_str:
                # This is the "Produce Time" (T1)
                latest_produce_time = datetime.fromisoformat(latest_produce_str)
                # This is the "Insert Time" (T2)
                latest_insert_time = datetime.fromisoformat(latest_insert_str)
                
                # Latency 1: Ingestion Latency (T2 - T1)
                ingestion_latency = latest_insert_time - latest_produce_time
                ingestion_latencies.append(ingestion_latency.total_seconds())
                print(f"  1. Producer-to-DB Latency: {ingestion_latency.total_seconds():.4f} seconds")

                # Latency 2: Aggregation Latency (T3 - T2)
                aggregation_latency = result_generation_time - latest_insert_time
                aggregation_latencies.append(aggregation_latency.total_seconds())
                print(f"  2. Aggregation Latency:      {aggregation_latency.total_seconds():.4f} seconds")

                # Latency 3: Total End-to-End Latency (T3 - T1)
                total_latency = result_generation_time - latest_produce_time
                total_latencies.append(total_latency.total_seconds())
                print(f"  -------------------------------------------------")
                print(f"  3. Total End-to-End Latency: {total_latency.total_seconds():.4f} seconds")

            else:
                print("  Could not calculate latencies (missing timestamps in payload).")

    except KeyboardInterrupt:
        print("\n\n--- Shutting Down Latency Analyzer ---")
    finally:
        consumer.close()
        if total_latencies:
            avg_total = sum(total_latencies) / len(total_latencies)
            print(f"\nAverage Total End-to-End Latency: {avg_total:.4f} seconds")
        if ingestion_latencies:
            avg_ingestion = sum(ingestion_latencies) / len(ingestion_latencies)
            print(f"  > Average Producer-to-DB Latency: {avg_ingestion:.4f} seconds")
        if aggregation_latencies:
            avg_aggregation = sum(aggregation_latencies) / len(aggregation_latencies)
            print(f"  > Average Aggregation Latency:      {avg_aggregation:.4f} seconds")

if __name__ == '__main__':
    run_latency_analyzer()



