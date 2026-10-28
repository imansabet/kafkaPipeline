
import json
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"


def on_delivery(error, message):
    key = message.key()
    key_text = key.decode("utf-8") if key else "(no key)"

    if error is not None:
        print(f"Delivery failed for {key_text}: {error}")
    else:
        print(
            f"Delivered {key_text} to partition "
            f"{message.partition()} at offset {message.offset()}"
        )


def main():
    with open("sample_orders.json", encoding="utf-8") as file:
        orders = json.load(file)

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS
    })

    for order in orders:
        producer.produce(
            TOPIC,
            key=str(order["order_id"]),
            value=json.dumps(order, ensure_ascii=False),
            on_delivery=on_delivery,
        )
        producer.poll(0)

    remaining = producer.flush(10)

    if remaining:
        print(f"Warning: {remaining} message(s) were not delivered")


if __name__ == "__main__":
    main()

