import csv
import random
import os
from datetime import datetime, timedelta

def main():
    file_path = "/data/cdr_data.csv"
    print(f"Generating CDR dataset at {file_path}...")
    
    # Define callers and receivers
    whale_caller = "whale_caller_999"
    other_callers = [f"caller_{i:04d}" for i in range(1000)]
    receivers = [f"receiver_{i:04d}" for i in range(1000)]
    towers = [f"tower_{i:03d}" for i in range(50)]
    call_types = ["VOICE", "SMS", "DATA"]
    
    start_time = datetime(2026, 5, 1, 0, 0, 0)
    
    # We want to generate exactly 2,050,000 records.
    # Whale caller should have exactly 210,000 records (10.24%).
    # Others should have 1,840,000 records.
    total_whale = 210000
    total_others = 1840000
    total_records = total_whale + total_others
    
    # Pre-assign mean and stddev for callers so that they are stable
    # caller_id -> (mean, stddev)
    caller_stats = {}
    for idx, c in enumerate(other_callers):
        mean = 120 + (idx % 10) * 30   # 120 to 390
        stddev = 15 + (idx % 5) * 5     # 15 to 35
        caller_stats[c] = (mean, stddev)
    
    # For whale caller
    caller_stats[whale_caller] = (150.0, 20.0)
    
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["caller_id", "receiver_id", "duration_sec", "tower_id", "timestamp", "call_type", "charge_amount"])
        
        whale_count = 0
        other_count = 0
        
        for i in range(total_records):
            # Decide if this is whale caller
            if whale_count < total_whale and (other_count >= total_others or random.random() < 0.105):
                caller = whale_caller
                whale_count += 1
            else:
                caller = random.choice(other_callers)
                other_count += 1
                
            mean, stddev = caller_stats[caller]
            
            # Duration: normally distributed
            # To generate anomalies, let's make 0.1% of calls extreme outliers
            if random.random() < 0.001:
                # Anomaly! E.g. mean + 5 * stddev
                duration = int(mean + 5 * stddev + random.randint(10, 50))
            else:
                duration = int(random.gauss(mean, stddev))
                if duration < 1:
                    duration = random.randint(1, 10)
            
            receiver = random.choice(receivers)
            tower = random.choice(towers)
            
            # Timestamp: random timestamp within 30 days
            offset_sec = random.randint(0, 30 * 24 * 3600)
            timestamp = (start_time + timedelta(seconds=offset_sec)).isoformat()
            
            call_type = random.choice(call_types)
            
            # Charge amount
            if call_type == "SMS":
                charge = 0.05
            else:
                charge = round(duration * 0.01 + random.uniform(0.01, 0.20), 2)
            
            writer.writerow([caller, receiver, duration, tower, timestamp, call_type, charge])
            
            if i > 0 and i % 500000 == 0:
                print(f"Generated {i} records...")
                
    print(f"Generation complete. Total records: {total_records}. Whale caller count: {whale_count} ({whale_count/total_records*100:.2f}%)")

if __name__ == "__main__":
    main()
