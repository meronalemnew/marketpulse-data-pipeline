import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer


project_folder = Path(__file__).resolve().parent
data_folder = project_folder / "data"
data_folder.mkdir(exist_ok=True)

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
output_file = data_folder / f"sample_trades_{run_id}.jsonl"

symbols = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL"]
random_generator = random.Random(42)

kafka_topic = "stock-trades"

producer = Producer(
    {
        "bootstrap.servers": "localhost:9092",
        "client.id": "marketpulse-collector",
        "enable.idempotence": True,
        "message.timeout.ms": 10000,
    }
)

delivery_errors = []


def delivery_report(error, message):
    """Record whether Kafka accepted each trade."""
    if error is not None:
        delivery_errors.append(str(error))
    else:
        print(
            f"Delivered to {message.topic()} "
            f"partition {message.partition()} "
            f"offset {message.offset()}"
        )


records_written = 0
started = time.perf_counter()

with output_file.open("x", encoding="utf-8") as file:
    for number in range(20):
        trade = {
            "source": "synthetic_sample",
            "trade_id": f"{run_id}-{number}",
            "symbol": symbols[number % len(symbols)],
            "price": round(random_generator.uniform(100, 200), 2),
            "size": random_generator.randint(1, 100),
            "event_time": datetime.now(timezone.utc).isoformat(),
        }

        trade_json = json.dumps(trade)

        # Preserve the raw trade locally
        file.write(trade_json + "\n")

        # Publish the same trade to Kafka
        producer.produce(
            topic=kafka_topic,
            key=trade["symbol"],
            value=trade_json,
            on_delivery=delivery_report,
        )

        producer.poll(0)
        records_written += 1

# Wait for Kafka to confirm delivery before exiting
messages_remaining = producer.flush(10)
elapsed = time.perf_counter() - started

print(f"Records saved locally: {records_written}")
print(f"File size: {output_file.stat().st_size} bytes")
print(f"Generation and publishing time: {elapsed:.6f} seconds")
print(f"Output file: {output_file}")

if messages_remaining or delivery_errors:
    print(f"Messages not delivered: {messages_remaining}")

    for error in delivery_errors:
        print(f"Kafka error: {error}")

    raise SystemExit(1)

print(f"Kafka records confirmed: {records_written}")