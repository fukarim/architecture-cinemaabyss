const { Kafka } = require('kafkajs');

class KafkaConsumer {
  constructor(brokers, groupId = 'events-service-group') {
    this.kafka = new Kafka({
      clientId: 'events-service',
      brokers: brokers.split(','),
    });
    this.consumer = this.kafka.consumer({ groupId });
    this.isConnected = false;
  }

  async connect() {
    if (!this.isConnected) {
      await this.consumer.connect();
      this.isConnected = true;
      console.log('Kafka Consumer connected');
    }
  }

  async subscribe(topics) {
    await this.connect();
    for (const topic of topics) {
      await this.consumer.subscribe({ topic, fromBeginning: false });
      console.log(`Subscribed to topic: ${topic}`);
    }
  }

  async start(handler) {
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const event = JSON.parse(message.value.toString());
          console.log(`[${new Date().toISOString()}] Received event from topic ${topic}:`, {
            id: event.id,
            type: event.type,
            partition,
            offset: message.offset,
          });

          if (handler) {
            await handler(topic, event, partition, message.offset);
          }
        } catch (error) {
          console.error('Error processing message:', error);
        }
      },
    });
  }

  async disconnect() {
    if (this.isConnected) {
      await this.consumer.disconnect();
      this.isConnected = false;
      console.log('Kafka Consumer disconnected');
    }
  }
}

module.exports = KafkaConsumer;
