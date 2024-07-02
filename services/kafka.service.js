const { Kafka, Partitioners } = require("kafkajs");

class KafkaService {
  constructor() {
    this.kafka = new Kafka({
      clientId: `transcode-engine`,
      brokers: [process.env.KAFKA_BROKER_URL],
      ssl: {
        host: process.env.KAFKA_HOST_URL,
      },
      sasl: {
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
        mechanism: "plain",
      },
    });
    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
    });
  }

  getClient() {
    return this.kafka;
  }

  async connectProducer() {
    try {
      await this.producer.connect();
    } catch (error) {
      console.error("Unable to connect Kafka Producer > ", error);
    }
  }

  async publishLog(log, metaData) {
    const { transcode_id, user_id } = metaData;
    try {
      await this.producer.send({
        topic: "container-logs",
        messages: [
          {
            key: `log-${user_id}-${transcode_id}`,
            value: JSON.stringify({
              transcode_id,
              user_id,
              log,
            }),
          },
        ],
      });
    } catch (error) {
      console.error("Unable to push log in Kafka > ", error);
    }
  }
}

module.exports = { KafkaService };
