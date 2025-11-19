

import json
import uuid
import random
import math  


AD_TYPES = ["banner", "video", "popup"]
EVENT_TYPES = ["view", "click", "purchase"]

def random_ip():
    return ".".join(str(random.randint(1, 254)) for _ in range(4))

def generate(kafka_producer, topic_name, producer_id, duration_sec, ad_mappings, per_producer_rate, window_size_sec):
    print(f"[P{producer_id}] Using POISSON generator (Exponential Timestamps) with avg rate (λ) of {per_producer_rate} events/sec.")
    
    for second in range(duration_sec):
        sec_base_ns = second * 1_000_000_000
        
       
        
        current_ns_in_second = 0
        num_events_this_second = 0
        
        while True:
            # 1. Calculate a random time gap until the next event
            # The time between events in a Poisson process follows an exponential distribution.
            # We multiply by 1 billion to get the gap in nanoseconds.
            time_gap_ns = -math.log(1.0 - random.random()) / per_producer_rate * 1_000_000_000
            
            # 2. Advance our nanosecond clock
            current_ns_in_second += time_gap_ns
            
            # 3. If the event is still within this second, create and produce it
            if current_ns_in_second < 1_000_000_000:
                event_time_ns = sec_base_ns + int(current_ns_in_second)
                chosen_ad_id, chosen_campaign_id = random.choice(ad_mappings)

                payload = {
                    "user_id": str(uuid.uuid4()), "page_id": str(uuid.uuid4()),
                    "ad_id": chosen_ad_id, "ad_type": random.choice(AD_TYPES),
                    "event_type": random.choice(EVENT_TYPES), "event_time_ns": int(event_time_ns),
                    "ip_address": random_ip(), "campaign_id": chosen_campaign_id,
                    "window_id": int(event_time_ns // (window_size_sec * 1_000_000_000))
                }

                kafka_producer.produce(
                    topic=topic_name, partition=producer_id,
                    key=str(payload["campaign_id"]), value=json.dumps(payload).encode("utf-8")
                )
                num_events_this_second += 1
            else:
                # The next event would be in the next second, so stop this loop
                break
        


        kafka_producer.flush()
        if (second + 1) % max(1, duration_sec // 10) == 0 or second < 3:
            print(f"[P{producer_id}] POISSON: second {second+1}/{duration_sec} emitted {num_events_this_second} events")
            
    print(f"[P{producer_id}] Poisson generator finished.")