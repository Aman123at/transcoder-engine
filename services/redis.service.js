const Redis = require("ioredis");
class RedisService {
  constructor() {
    this.redisClient = new Redis({
      port: process.env.REDIS_PORT, // Redis port
      host: process.env.REDIS_HOST, // Redis host
      username: process.env.REDIS_USERNAME, // needs Redis >= 6
      password: process.env.REDIS_PASSWORD,
    });
  }
  async isRedisClientAvailable() {
    const reponse = await this.redisClient.ping();
    return reponse === "PONG";
  }

  async releaseCurrentOccupiedSlotInQueue() {
    const occupiedSlots = await this.redisClient.get("occupiedSlots");
    if (occupiedSlots) {
      await this.redisClient.set("occupiedSlots", occupiedSlots - 1);
    }
  }

  async getDataFromKey(key) {
    const data = await this.redisClient.get(key);
    return JSON.parse(data);
  }

  async removeKey(key) {
    await this.redisClient.del(key);
  }

  async removeVideoMetaDataFromRedisQueue(queue_name, file) {
    try {
      await this.redisClient.lrem(queue_name, 0, file); // 0 means remove all occurrences
    } catch (error) {
      console.log("Unable to remove video from queue", error);
    }
  }
}
module.exports = { RedisService };
