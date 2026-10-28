import json 
import zlib

NUM_PARTITIONS = 2

with open("concept_orders.json") as file:
    orders = json.load(file)

partitions = [[] for _ in range(NUM_PARTITIONS)]

print("PRODUCER: sending events to the order topic")
for order in orders : 
    key = order["order_id"]
    partition = zlib.crc32(key.encode()) % NUM_PARTITIONS
    offset = len(partitions[partition])
    partitions[partition].append(order)
    print(f" {key} ({order['status']}) -> partition {partition} , offset {offset}")


print()
print("CONSUMER: reading each partition from the start")
for number in range(NUM_PARTITIONS):
    for offset, order in enumerate(partitions[number]):
        print(f" partition {number}, offset {offset}: {order['customer']} ordered {order['quantity']} x {order['item']} ({order['status']})")

print()
for number in range(NUM_PARTITIONS):
    print(f"Events still stored in partitio {number}: {len(partitions[number])}")


print()
for number in range(NUM_PARTITIONS):
    print(f"Events still stored in partition {number}: {len(partitions[number])}")
