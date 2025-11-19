

import json
import uuid
import random
import math  # Using math for the logarithm function

# Common payload fields
AD_TYPES = ["banner", "video", "popup"]
EVENT_TYPES = ["view", "click", "purchase"]

def random_ip():
    return ".".join(str(random.randint(1, 254)) for _ in range(4))

def generate(kafka_producer, topic_name, producer_id, duration_sec, ad_mappings, per_producer_rate, window_size_sec):
    # --- MMPP Configuration ---
    d_H = 2; d_L = 8; factor = 4.0
    
    
    T = per_producer_rate
    lambda_H = factor * T
    lambda_L = (T * (d_H + d_L) - lambda_H * d_H) / d_L if d_L > 0 else 0

    if lambda_L < 0:
        raise ValueError(f"Invalid MMPP parameters for T={T}: λ_L is negative ({lambda_L:.2f}). Try decreasing 'factor'.")

    print(f"[P{producer_id}] Using MMPP generator (Exponential Timestamps). Avg Rate: {T}, High Rate (λ_H): {lambda_H:.2f}, Low Rate (λ_L): {lambda_L:.2f}")

    for second in range(duration_sec):
        current_cycle_time = second % (d_H + d_L)
        is_high_state = current_cycle_time < d_H
        current_rate = lambda_H if is_high_state else lambda_L
        state_name = "HIGH" if is_high_state else "LOW"

        sec_base_ns = second * 1_000_000_000
        
        
        
        current_ns_in_second = 0
        num_events_this_second = 0

        # Only generate events if the rate for the current state is positive
        if current_rate > 0:
            while True:
                # Calculate a random time gap based on the current state's rate (λ_H or λ_L)
                time_gap_ns = -math.log(1.0 - random.random()) / current_rate * 1_000_000_000
                current_ns_in_second += time_gap_ns
                
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
                    break
        

        kafka_producer.flush()
        if (second + 1) % max(1, duration_sec // 10) == 0 or second < 3:
            print(f"[P{producer_id}] MMPP ({state_name}): second {second+1}/{duration_sec} emitted {num_events_this_second} events")

    print(f"[P{producer_id}] MMPP generator finished.")