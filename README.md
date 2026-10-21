# MarketPulse Real-Time Data Pipeline

MarketPulse is a local end-to-end streaming data engineering project that generates synthetic stock trades, publishes them to Apache Kafka, processes them with Spark Structured Streaming, and stores validated records in PostgreSQL.

## Architecture

```text
Python Producer
      ↓
Apache Kafka
      ↓
Spark Structured Streaming
      ↓
PostgreSQL
```

## Features

- Generates synthetic trades for AAPL, MSFT, NVDA, AMZN, and GOOGL
- Publishes trade events to a partitioned Kafka topic
- Parses and validates JSON events with Spark Structured Streaming
- Rejects records with missing identifiers, invalid timestamps, or non-positive values
- Calculates trade value and processing latency
- Writes micro-batches to PostgreSQL through JDBC
- Uses Spark checkpoints for incremental processing and recovery
- Prevents duplicates with primary-key and Kafka-position constraints
- Runs Kafka, Spark, and PostgreSQL with Docker Compose
- Exposes the Spark monitoring interface on port 4040

## Technology Stack

- Python 3.12
- Apache Kafka 4.3.1
- Apache Spark 4.2.0
- PostgreSQL 16
- Docker and Docker Compose
- PySpark Structured Streaming
- PostgreSQL JDBC

## Project Structure

```text
marketpulse-data-pipeline/
├── collector.py
├── spark_consumer.py
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
├── sql/
│   └── init.sql
├── data/
└── checkpoints/
```

Generated JSONL files, Spark checkpoints, credentials, and local virtual-environment files are excluded from Git.

## Trade Schema

Each trade contains:

| Field | Description |
|---|---|
| `trade_id` | Unique trade identifier |
| `source` | Event source |
| `symbol` | Stock symbol |
| `price` | Trade price |
| `size` | Number of shares |
| `trade_value` | Calculated as price multiplied by size |
| `event_time` | Time the trade was generated |
| `kafka_timestamp` | Time Kafka received the event |
| `processed_at` | Time Spark processed the event |
| `processing_latency_seconds` | Difference between processing and event time |
| `kafka_partition` | Kafka partition |
| `kafka_offset` | Kafka offset |

## Data Quality Controls

The PostgreSQL table enforces:

- Unique `trade_id`
- Unique Kafka partition and offset combination
- Positive price
- Positive trade size
- Positive trade value
- Required identifiers and timestamps

## Local Setup

### 1. Create the Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Create the environment file

```bash
cp .env.example .env
```

Replace the example password in `.env` with a secure local password.

### 3. Start Kafka and PostgreSQL

```bash
docker compose up -d kafka postgres
```

### 4. Create the PostgreSQL schema

```bash
docker compose exec -T postgres \
  psql -U marketpulse -d marketpulse \
  < sql/init.sql
```

### 5. Start Spark Structured Streaming

```bash
docker compose up -d spark
```

### 6. Generate and publish trades

```bash
python collector.py
```

Spark automatically processes the new Kafka messages and writes them to PostgreSQL.

## Verify the Pipeline

Check the services:

```bash
docker compose ps
```

Check the stored record count:

```bash
docker compose exec postgres \
  psql -U marketpulse -d marketpulse \
  -c "SELECT
          COUNT(*) AS total_trades,
          COUNT(DISTINCT trade_id) AS unique_trades
      FROM trades;"
```

Inspect recent trades:

```bash
docker compose exec postgres \
  psql -U marketpulse -d marketpulse \
  -c "SELECT trade_id, symbol, price, size, trade_value, event_time
      FROM trades
      ORDER BY event_time DESC
      LIMIT 5;"
```

## Monitoring

While Spark is running, open:

```text
http://localhost:4040
```

The Spark interface displays:

- Active streaming queries
- Input and processing rates
- Batch duration
- Completed jobs, stages, and tasks

## Local Test Results

During the validated local test:

- 80 trades were stored
- 80 trade IDs were unique
- No duplicates were created
- A new batch was processed without restarting Spark
- Average Spark pickup latency for the optimized batch was 1.97 seconds
- The JDBC writer used one partition for efficient small-batch inserts

## Notes

This project uses synthetic stock-market data for educational and portfolio purposes. It does not provide real-time investment data or financial advice.