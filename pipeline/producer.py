import json
import sys

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
ORDERS_FILE = sys.argv[1] if len(sys.argv) > 1 else "sample_orders.json"

with open(ORDERS_FILE) as file:
    orders = json.load(file)

producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "message.timeout.ms": 10000,
    }
)


def on_delivery(error, message):
    if error is not None:
        print(f"Delivery failed for {message.key().decode()}: {error}")
    else:
        print(f"Delivered {message.key().decode()} to topic {message.topic()}, partition {message.partition()}, offset {message.offset()}")


for order in orders:
    producer.produce(
        TOPIC,
        key=order["order_id"],
        value=json.dumps(order),
        on_delivery=on_delivery,
    )

producer.flush()
