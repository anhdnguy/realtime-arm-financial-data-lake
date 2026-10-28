# Streaming Arm to Financial Data Lake

## Receiver
Takes live trades from Alpaca's websocket (IEX feed) and writes them to Kafka.

### How it works
- websocket → in-memory queue → Kafka, one trade at a time, waiting for the broker's ack
- every 5 s, the last trade time per ticker (the "mark") is saved to DynamoDB, so missed trades can be fetched again later
- bad records go to S3 (the DLQ)
- errors are handled by whose fault they are: bad data → DLQ, network → retry, setup → crash

### Run it
Prerequisites:
- Docker
- the external network: `docker network create shared-network`
- LocalStack running, with the DynamoDB table and S3 bucket created
- a `.env` file with `ALPACA_API_KEY`, `ALPACA_API_SECRET`, `ALPACA_URI`, `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `DYNAMODB_TABLE_NAME`, `S3_BUCKET`
  (`KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC` are optional, defaults `broker:29092` and `trades`)

```bash
docker compose up --build receiver
```
Starts the Kafka broker and creates the topic first.

### Check it works
Watch trades arrive (ticker, then the trade):
```bash
docker exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic trades --formatter-property print.key=true
```

Run the tests (they run in Docker):
```bash
./run_tests.sh
```
