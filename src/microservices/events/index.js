const express = require('express');
const bodyParser = require('body-parser');
const KafkaProducer = require('./producer');
const KafkaConsumer = require('./consumer');
const createRoutes = require('./routes');

const PORT = process.env.PORT || 8082;
const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'localhost:9092';

const app = express();

app.use(bodyParser.json());

const producer = new KafkaProducer(KAFKA_BROKERS);

const consumer = new KafkaConsumer(KAFKA_BROKERS);

const eventHandler = (topic, event, partition, offset) => {
  console.log(`[CONSUMER] Processing event from ${topic}:`);
  console.log(`  - Event ID: ${event.id}`);
  console.log(`  - Event Type: ${event.type}`);
  console.log(`  - Timestamp: ${event.timestamp}`);
  console.log(`  - Partition: ${partition}`);
  console.log(`  - Offset: ${offset}`);
  console.log(`  - Payload:`, JSON.stringify(event.payload, null, 2));
  console.log('---');
};

const routes = createRoutes(producer);
app.use('/api/events', routes);

const shutdown = async () => {
  try {
    await producer.disconnect();
    await consumer.disconnect();
    process.exit(0);
  } catch (error) {
    console.error('Error during shutdown:', error);
    process.exit(1);
  }
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

async function start() {
  try {
    await producer.connect();

    await consumer.subscribe(['movie-events', 'user-events', 'payment-events']);

    consumer.start(eventHandler);
    console.log('Kafka Consumer started and listening to topics');

    app.listen(PORT);
  } catch (error) {
    console.error('Failed to start service:', error);
    process.exit(1);
  }
}

start();
