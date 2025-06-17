from confluent_kafka import Producer
import json
import socket

conf = {
    'bootstrap.servers': "localhost:9092",
    'client.id': socket.gethostname()
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_price_event(event: dict):
    producer.produce(
        topic='price-events',
        key=event['symbol'],
        value=json.dumps(event),
        callback=delivery_report
    )
    producer.flush()

if __name__ == "__main__":
    test_event = {
        "symbol": "AAPL",
        "price": 186.32,
        "timestamp": "2025-06-17T13:05:44Z"
    }
    produce_price_event(test_event)
