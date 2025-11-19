import json
import uuid
import random


AD_TYPES = ["banner", "video", "popup"]
EVENT_TYPES = ["view", "click", "purchase"]

def random_ip():
    """Generates a random IP address."""
    return ".".join(str(random.randint(1, 254)) for _ in range(4))

def generate(kafka_producer, topic_name, producer_id, duration_sec, ad_mappings, per_producer_rate, window_size_sec):
    """
    Generates events at a perfectly uniform, deterministic rate within each simulated second.
    This is your original event generation logic, refactored into a function.
    """
    print(f"[P{producer_id}] Using UNIFORM generator at {per_producer_rate} events/sec.")
    
    # The time between events is fixed and calculated precisely
    interval_ns_total = max(1, int(1_000_000_000 // max(1, per_producer_rate)))

    for second in range(duration_sec):
        sec_base_ns = second * 1_000_000_000
        
        for local_idx in range(per_producer_rate):
            event_time_ns = sec_base_ns + local_idx * interval_ns_total

            chosen_ad_id, chosen_campaign_id = random.choice(ad_mappings)

            payload = {
                "user_id": str(uuid.uuid4()),
                "page_id": str(uuid.uuid4()),
                "ad_id": chosen_ad_id,
                "ad_type": random.choice(AD_TYPES),
                "event_type": random.choice(EVENT_TYPES),
                "event_time_ns": int(event_time_ns),
                "ip_address": random_ip(),
                "campaign_id": chosen_campaign_id,
                "window_id": int(event_time_ns // (window_size_sec * 1_000_000_000))
            }

            kafka_producer.produce(
                topic=topic_name,
                partition=producer_id,
                key=str(payload["campaign_id"]),
                value=json.dumps(payload).encode("utf-8")
            )


        kafka_producer.flush()
        if (second + 1) % max(1, duration_sec // 10) == 0 or second < 3:
            print(f"[P{producer_id}] UNIFORM: second {second+1}/{duration_sec} emitted {per_producer_rate} events")

    print(f"[P{producer_id}] Uniform generator finished.")
