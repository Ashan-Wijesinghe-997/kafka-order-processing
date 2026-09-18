# Kafka Order Processing System

This repository contains a Kafka-based system that produces and consumes order messages.
It satisfies the assignment requirements:
- Avro serialization via Schema Registry.
- Real-time aggregation (running average of prices).
- Retry logic for temporary failures (simulated).
- Dead Letter Queue (DLQ) for permanently failed messages.
- Written in Python.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (to run Kafka locally)
- [Python 3.8+](https://www.python.org/downloads/)

## Setup Instructions

1. **Start the Kafka Infrastructure**
   Open a terminal in this directory and run:
   ```bash
   docker-compose up -d
   ```
   *This starts Zookeeper, Kafka, and Confluent Schema Registry in the background.*

2. **Install Python Dependencies**
   Run the following command to install the required libraries:
   ```bash
   pip install -r requirements.txt
   ```

## Running the Application (Terminal Mode)

To demonstrate the system in the terminal, you will need two separate terminal windows.

### 1. Start the Consumer
In the first terminal, run:
```bash
python consumer.py
```
*The consumer will start listening to the `orders` topic.*

### 2. Start the Producer
In the second terminal, run:
```bash
python producer.py
```
*The producer will generate randomized order messages with Avro serialization and send them to the `orders` topic.*

## Running the Dashboard (Frontend Mode)

We have built a beautiful, real-time web dashboard to demonstrate the system live.

### 1. Start the Web Server
In your terminal, run:
```bash
uvicorn app:app
```
*This starts the FastAPI web server and background consumer.*

### 2. Open the Dashboard
Open your web browser and navigate to:
[http://localhost:8000](http://localhost:8000)

### 3. Start the Producer
Open a new terminal window and run:
```bash
python producer.py
```
*Watch the browser dashboard update in real-time as orders flow through Kafka!*

## How to Verify the Requirements

- **Real-time Aggregation:** As the producer sends messages, watch the Consumer terminal output. You will see it computing and printing the `Running Average Price`.
- **Retry Logic:** The consumer randomly simulates temporary failures. When this happens, you will see output like `-> Processing failed temporarily. Retry 1/3...` before it succeeds.
- **Dead Letter Queue (DLQ):** The consumer also randomly simulates permanent failures or max retries exceeded. When this happens, it will output `-> Processing failed permanently. Moving to DLQ.` and the raw message is sent to the `orders-dlq` topic.

## Cleanup

When you are finished, stop the consumer and producer using `Ctrl+C`.
To tear down the Kafka infrastructure, run:
```bash
docker-compose down
```
