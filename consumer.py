from confluent_kafka import Consumer
import json
from dlq_producer import send_to_dlq


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "orders-dedup-practice",  # name of the consumer group
    "auto.offset.reset": "earliest",  # what to do when the consumer has no committed offset
    "enable.auto.commit": False, # disables automatic offset commits.
    "enable.auto.offset.store": False, # disables automatic storing of the latest consumed position.
})

consumer.subscribe(["orders"])
processed_events = set()


def process_order(order):
    max_retries = 3

    for attempt in range(max_retries):
        try:
            if attempt < 3:
                raise Exception("Database temporarily unavailable")
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
        else:
            print("Order processed successfully")
            return True

    print("All retries failed")
    return False


try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print("waiting for messages...")
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue

        msg_decoded = msg.value().decode("utf-8")
        print("Raw message:", repr(msg_decoded))

        try:
            val = json.loads(msg_decoded)
        except json.JSONDecodeError as e:
            print("Invalid JSON:", e)
            continue

        if val["event_id"] in processed_events:
            print("value skipped, duplication detected")
            consumer.commit(message=msg, asynchronous=False)
        else:
            print(val["event_id"], val["amount"])
            success = process_order(val)
            if success:
                consumer.commit(message=msg, asynchronous=False)
                processed_events.add(val["event_id"])
            else:
                print("Processing failed. Sending event to DLQ...")

                try:
                    send_to_dlq(
                        val,
                        "Processing failed after all retries",
                        msg
                    )

                    print("DLQ delivery confirmed. Committing original offset.")
                    consumer.commit(message=msg, asynchronous=False)

                except Exception as e:
                    print(f"DLQ delivery failed: {e}")
                    print("Original offset NOT committed.")
                    break
except KeyboardInterrupt:
    print("Exiting...")
finally:
    consumer.close()
   