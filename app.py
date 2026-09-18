import asyncio
import random
import json
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from confluent_kafka import DeserializingConsumer, Producer, KafkaError
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# Configuration
KAFKA_BROKER = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
TOPIC_NAME = 'orders'
DLQ_TOPIC = 'orders-dlq'
GROUP_ID = f'order-processing-web-{uuid.uuid4()}'  # Unique group ID so we always get latest messages in demo
MAX_RETRIES = 3

class RunningAverageCalculator:
    def __init__(self):
        self.total_sum = 0.0
        self.count = 0

    def add_price(self, price):
        self.total_sum += price
        self.count += 1
        return self.total_sum / self.count

# Global state
avg_calculator = RunningAverageCalculator()
consumer_task = None

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        # Send current state on connect
        current_avg = 0 if avg_calculator.count == 0 else avg_calculator.total_sum / avg_calculator.count
        await websocket.send_text(json.dumps({"type": "init", "average": current_avg}))

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        if not self.active_connections:
            return
        json_msg = json.dumps(message)
        dead_connections = []
        for connection in self.active_connections:
            try:
                await connection.send_text(json_msg)
            except Exception:
                dead_connections.append(connection)
        for dc in dead_connections:
            self.disconnect(dc)

manager = ConnectionManager()

def simulate_processing_failure():
    rand = random.random()
    if rand < 0.1:      # 10% chance of temporary failure
        return 1
    elif rand < 0.15:   # 5% chance of permanent failure
        return 2
    return 0

def send_to_dlq(producer, key, value_bytes, error_msg):
    try:
        producer.produce(
            topic=DLQ_TOPIC,
            key=key,
            value=str(value_bytes) + f" | ERROR: {error_msg}"
        )
        producer.poll(0)
    except Exception as e:
        print(f"Failed to send message to DLQ: {e}")

async def consume_kafka():
    loop = asyncio.get_running_loop()
    
    # Setup Schema Registry client
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Read Avro schema
    with open('order.avsc', 'r') as f:
        schema_str = f.read()

    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
        from_dict=lambda obj, ctx: obj
    )

    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'latest', # Only show new messages in the web UI demo
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer
    }
    
    try:
        consumer = DeserializingConsumer(consumer_conf)
        consumer.subscribe([TOPIC_NAME])
    except Exception as e:
        print(f"Failed to start consumer: {e}")
        return

    dlq_producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    print(f"Web Consumer started... Listening to topic '{TOPIC_NAME}'")

    try:
        while True:
            # Run poll in executor to not block asyncio loop
            msg = await loop.run_in_executor(None, consumer.poll, 0.5)
            
            if msg is None:
                await asyncio.sleep(0.1)
                continue
            
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {msg.error()}")
                continue
                
            order = msg.value()
            key = msg.key()
            
            # Broadcast received order
            await manager.broadcast({
                "type": "order_received",
                "order": order
            })
            
            retries = 0
            success = False
            
            while retries <= MAX_RETRIES:
                status = simulate_processing_failure()
                
                if status == 0:
                    running_avg = avg_calculator.add_price(order['price'])
                    await manager.broadcast({
                        "type": "order_success",
                        "orderId": order['orderId'],
                        "average": running_avg
                    })
                    success = True
                    break
                
                elif status == 1:
                    retries += 1
                    await manager.broadcast({
                        "type": "order_retry",
                        "orderId": order['orderId'],
                        "retry_count": retries,
                        "max_retries": MAX_RETRIES
                    })
                    await asyncio.sleep(0.5) # Simulate slight delay on retry
                    
                elif status == 2:
                    send_to_dlq(dlq_producer, key, order, "Simulated permanent failure")
                    await manager.broadcast({
                        "type": "order_dlq",
                        "orderId": order['orderId'],
                        "reason": "Permanent Processing Failure"
                    })
                    success = True
                    break
            
            if not success and retries > MAX_RETRIES:
                send_to_dlq(dlq_producer, key, order, "Max retries exceeded")
                await manager.broadcast({
                    "type": "order_dlq",
                    "orderId": order['orderId'],
                    "reason": "Max Retries Exceeded"
                })
                
    except asyncio.CancelledError:
        print("Consumer task cancelled")
    finally:
        consumer.close()
        dlq_producer.flush()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global consumer_task
    consumer_task = asyncio.create_task(consume_kafka())
    yield
    # Shutdown
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

app = FastAPI(lifespan=lifespan)

# Mount static files
import os
os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def get_index():
    return FileResponse("static/index.html")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # We don't expect client to send anything, but we need to keep connection open
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
