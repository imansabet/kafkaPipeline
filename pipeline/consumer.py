import json
import sys

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
GROUP_ID = sys.argv[1] if len(sys.argv) > 1 else "order-notifications"
CLIENT_ID = sys.argv[2] if len(sys.argv) > 2 else "consumer-1"

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "client.id": CLIENT_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }
)


def on_assign(consumer, partitions):
    print(f"[{CLIENT_ID}] Assigned partitions: {[partition.partition for partition in partitions]}")


consumer.subscribe([TOPIC], on_assign=on_assign)

try:
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        order = json.loads(message.value().decode())
        print(
            f"[partition {message.partition()}, offset {message.offset()}] "
            f"Notification for {order['customer']}: order {order['order_id']} "
            f"({order['quantity']} x {order['item']}) is being packed for shipment to {order['shipping_city']}."
        )
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
