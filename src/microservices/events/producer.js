const { Kafka } = require('kafkajs');

class KafkaProducer {
  constructor(brokers) {
    this.kafka = new Kafka({
      clientId: 'events-service',
      brokers: brokers.split(','),
    });
    this.producer = this.kafka.producer();
    this.isConnected = false;
  }

  async connect() {
    if (!this.isConnected) {
      await this.producer.connect();
      this.isConnected = true;
      console.log('Kafka Producer connected');
    }
  }

  async sendEvent(topic, event) {
    try {
      await this.connect();

      const result = await this.producer.send({
        topic: topic,
        messages: [
          {
            key: event.id,
            value: JSON.stringify(event),
            timestamp: new Date().getTime().toString(),
          },
        ],
      });

      console.log(`Event sent to topic ${topic}:`, event.id);
      return result[0];
    } catch (error) {
      console.error('Error sending event:', error);
      throw error;
    }
  }

  async disconnect() {
    if (this.isConnected) {
      await this.producer.disconnect();
      this.isConnected = false;
      console.log('Kafka Producer disconnected');
    }
  }
}

module.exports = KafkaProducer;
