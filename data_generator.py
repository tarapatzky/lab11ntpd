import os
import time
import random
import pandas as pd
from datetime import datetime, timedelta

OUTPUT_DIR = "data/input_stream"
os.makedirs(OUTPUT_DIR, exist_ok=True)

categories = ["books", "electronics", "clothing", "tools", "groceries"]
statuses = ["paid", "paid", "paid", "failed"]

print("Uruchamiam generator danych. Naciśnij Ctrl+C, aby zatrzymać.")

file_counter = 1
while True:
    records = []
    for _ in range(random.randint(1, 5)):
        event_time = datetime.now() - timedelta(seconds=random.randint(0, 15))
        records.append({
            "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": f"u{random.randint(1, 100):03d}",
            "category": random.choice(categories),
            "amount": round(random.uniform(10.0, 500.0), 2),
            "status": random.choice(statuses)
        })

    df = pd.DataFrame(records)
    file_path = os.path.join(OUTPUT_DIR, f"batch_{file_counter:04d}.csv")
    df.to_csv(file_path, index=False)

    print(f"Wygenerowano plik: {file_path} z {len(df)} rekordami.")
    file_counter += 1
    time.sleep(5)