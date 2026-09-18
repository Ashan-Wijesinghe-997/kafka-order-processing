import sys
import random
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import Producer

# Configuration
KAFKA_BROKER = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC_NAME = 'orders'
DLQ_TOPIC = 'orders-dlq'
GROUP_ID = 'order-processing-group'
MAX_RETRIES = 3

class RunningAverageCalculator:
    def __init__(self):
        self.total_sum = 0.0
        self.count = 0

    def add_price(self, price):
        self.total_sum += price
        self.count += 1
        return self.total_sum / self.count

def simulate_processing_failure():
    """
    Simulates random processing failures.
    Returns:
        0 - Success
        1 - Temporary Failure
        2 - Permanent Failure
    """
    rand = random.random()
    if rand < 0.1:      # 10% chance of temporary failure
        return 1
    elif rand < 0.15:   # 5% chance of permanent failure
        return 2
    return 0

def send_to_dlq(producer, key, value_bytes, error_msg):
    """Sends permanently failed messages to the DLQ (Dead Letter Queue)."""
    # Note: Value is sent as raw bytes because it couldn't be processed normally
    try:
        producer.produce(
            topic=DLQ_TOPIC,
            key=key,
            value=str(value_bytes) + f" | ERROR: {error_msg}"
        )
        producer.poll(0)
        print(f"-> Sent message with key {key} to DLQ.")
    except Exception as e:
        print(f"Failed to send message to DLQ: {e}")

def main():
    # Setup Schema Registry client
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Read Avro schema
    with open('order.avsc', 'r') as f:
        schema_str = f.read()

    # Create Avro Deserializer
    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
        from_dict=lambda obj, ctx: obj
    )

    # Configure Consumer
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer
    }
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([TOPIC_NAME])

    # Configure raw string producer for DLQ
    dlq_producer = Producer({'bootstrap.servers': KAFKA_BROKER})

    avg_calculator = RunningAverageCalculator()

    print(f"Starting consumer... Listening to topic '{TOPIC_NAME}'")
    try:
        while True:
            # Poll for messages
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
                
            order = msg.value()
            key = msg.key()
            
            print(f"\nReceived Order: {order}")
            
            # Retry logic
            retries = 0
            success = False
            
            while retries <= MAX_RETRIES:
                status = simulate_processing_failure()
                
                if status == 0:
                    # Success
                    running_avg = avg_calculator.add_price(order['price'])
                    print(f"-> Processing SUCCESS. Current Running Average Price: ${running_avg:.2f}")
                    success = True
                    break
                
                elif status == 1:
                    # Temporary failure
                    retries += 1
                    print(f"-> Processing failed temporarily. Retry {retries}/{MAX_RETRIES}...")
                    
                elif status == 2:
                    # Permanent failure
                    print(f"-> Processing failed permanently. Moving to DLQ.")
                    send_to_dlq(dlq_producer, key, order, "Simulated permanent failure")
                    success = True # Considered handled via DLQ
                    break
            
            if not success and retries > MAX_RETRIES:
                print(f"-> Max retries exceeded. Moving to DLQ.")
                send_to_dlq(dlq_producer, key, order, "Max retries exceeded")
                
    except KeyboardInterrupt:
        print("Interrupt received, stopping consumer...")
    finally:
        consumer.close()
        dlq_producer.flush()
        print("Consumer stopped.")

if __name__ == '__main__':
    # Using 'False' syntax fallback if true/false wasn't imported/capitalized correctly
    main()
