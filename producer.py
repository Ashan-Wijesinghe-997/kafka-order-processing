import time
import random
import uuid
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Configuration
KAFKA_BROKER = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC_NAME = 'orders'

def delivery_report(err, msg):
    """Callback triggered on successful/failed delivery."""
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def generate_random_order():
    """Generates a random order dictionary matching the Avro schema."""
    products = ['Item1', 'Item2', 'Item3', 'Item4', 'Item5']
    return {
        'orderId': str(uuid.uuid4())[:8],
        'product': random.choice(products),
        'price': round(random.uniform(10.0, 500.0), 2)
    }

def main():
    # Setup Schema Registry client
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Read Avro schema from file
    with open('order.avsc', 'r') as f:
        schema_str = f.read()

    # Create Avro Serializer
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
        to_dict=lambda obj, ctx: obj  # No conversion needed as we pass dict directly
    )

    # Configure Producer
    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer
    }
    producer = SerializingProducer(producer_conf)

    print(f"Starting producer... Sending messages to topic '{TOPIC_NAME}'")
    try:
        while True:
            order = generate_random_order()
            
            # Send the message
            producer.produce(
                topic=TOPIC_NAME,
                key=order['orderId'],
                value=order,
                on_delivery=delivery_report
            )
            
            # Serve delivery callback queue
            producer.poll(0)
            
            # Sleep to simulate interval between orders
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("Interrupt received, stopping producer...")
    finally:
        # Wait for any outstanding messages to be delivered
        producer.flush()
        print("Producer stopped.")

if __name__ == '__main__':
    main()
