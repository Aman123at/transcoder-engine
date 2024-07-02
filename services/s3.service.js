const {
  PutObjectCommand,
  S3Client,
  DeleteObjectCommand,
} = require("@aws-sdk/client-s3");

class S3Service {
  constructor() {
    this.s3Client = new S3Client({
      region: "ap-south-1",
      credentials: {
        accessKeyId: process.env.AWS_S_THREE_ACCESSKEYID,
        secretAccessKey: process.env.AWS_S_THREE_SECRETACCESSKEY,
      },
    });
  }
  getClient() {
    return this.s3Client;
  }
  generatePutObjectCommand(payload) {
    return new PutObjectCommand(payload);
  }

  removeFileFromTempBucketCommand() {
    const params = {
      Bucket: process.env.TEMP_BUCKET_NAME,
      Key: process.env.FILE_KEY,
    };
    const command = new DeleteObjectCommand(params);
    return command;
  }

  async executeCommand(command) {
    await this.s3Client.send(command);
  }
}

module.exports = { S3Service };
