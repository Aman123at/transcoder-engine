const { MongoClient } = require("mongodb");
class MongoService {
  constructor(mongourl) {
    this.client = new MongoClient(mongourl);
  }

  async updateVideoDuration(transcode_id, duration) {
    try {
      await this.client.connect();
      const database = this.client.db();
      const collection = database.collection("videos"); // Replace with your collection name

      const filter = { transcode_id }; // Filter condition to find the document to update
      const updateDoc = {
        $set: {
          video_duration: duration,
        },
      };

      const result = await collection.updateOne(filter, updateDoc);

      console.log(`${result.modifiedCount} document(s) updated`);
    } catch (error) {
      console.log("Error updating video duration:", error);
    } finally {
      await this.client.close();
    }
  }
}

module.exports = { MongoService };
