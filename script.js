const { exec } = require("child_process");
const fs = require("fs");
const path = require("path");
const mime = require("mime-types");
const { S3Service } = require("./services/s3.service.js");
const { KafkaService } = require("./services/kafka.service.js");
const { RedisService } = require("./services/redis.service.js");
const { MongoService } = require("./services/mongo.service.js");
const VIDEO_INPUT_URL = process.env.VIDEO_INPUT_URL;
const FILE_KEY = process.env.FILE_KEY;
const kafkaService = new KafkaService();
const s3Service = new S3Service();
const redisService = new RedisService();
const mongoUri = `mongodb+srv://${process.env.MONGO_USER}:${process.env.MONGO_PASS}@cluster0.ofkak2x.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0`;
const mongoService = new MongoService(mongoUri);

// release the slot in redis once video is transcoded
async function releaseOccupiedSlot(fileMetaData) {
  try {
    const isRedisClientAvailable = await redisService.isRedisClientAvailable();
    if (isRedisClientAvailable) {
      await redisService.releaseCurrentOccupiedSlotInQueue();
      console.log("Released slot successfully.");
      await kafkaService.publishLog(
        "Released slot successfully.",
        fileMetaData
      );
    }
  } catch (error) {
    console.log("Redis connectivity error at releaseOccupiedSlot()");
    await kafkaService.publishLog(
      "Redis connectivity error at releaseOccupiedSlot()",
      fileMetaData
    );
  }
}

// remove file key with metadata from redis after transcoding is done
async function removeFileKeyFromRedis(fileMetaData) {
  try {
    const isRedisClientAvailable = await redisService.isRedisClientAvailable();
    if (isRedisClientAvailable) {
      await redisService.removeKey(FILE_KEY);
      console.log("Successfully removed file from Redis.");
      await kafkaService.publishLog(
        "Successfully removed file from Redis.",
        fileMetaData
      );
    }
  } catch (error) {
    console.log("Redis connectivity error at removeFileKeyFromRedis()");
    await kafkaService.publishLog(
      "Redis connectivity error at removeFileKeyFromRedis()",
      fileMetaData
    );
  }
}

async function removeFileFromTempBucket(fileMetaData) {
  try {
    const command = s3Service.removeFileFromTempBucketCommand();
    await s3Service.executeCommand(command);
    console.log("Successfully removed file from Temporary bucket.");
    await kafkaService.publishLog(
      "Successfully removed file from Temporary bucket.",
      fileMetaData
    );
  } catch (error) {
    console.log("Error removing Temporary bucket file.");
    await kafkaService.publishLog(
      "Error removing Temporary bucket file.",
      fileMetaData
    );
  }
}

async function main() {
  await kafkaService.connectProducer();

  try {
    const isRedisClientAvailable = await redisService.isRedisClientAvailable();

    if (isRedisClientAvailable) {
      const fileMetaData = await redisService.getDataFromKey(FILE_KEY);
      if (fileMetaData && Object.keys(fileMetaData).length) {
        const { transcode_id, user_id } = fileMetaData;
        const findDurationCommand = `ffprobe -i ${VIDEO_INPUT_URL} -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1`;
        exec(findDurationCommand, async (error, stdout, stderr) => {
          if (error) {
            console.log("Error finding duration of video", error);
          }
          if (stderr) {
            console.log("Error finding duration of video", stderr);
          }
          if (stdout) {
            console.log("Duration of video", stdout);
            await mongoService.updateVideoDuration(
              transcode_id,
              parseInt(stdout)
            );
          }
        });
        const ffmpegCommand = `ffmpeg -i ${VIDEO_INPUT_URL} -codec:a aac -strict -2 -codec:v:0 libx264 -filter:v:0 scale=w=640:h=360 -b:v:0 800k -maxrate:v:0 856k -bufsize:v:0 1200k -crf 20 -g 48 -hls_time 10 -hls_playlist_type vod -hls_segment_filename "outputs/${user_id}/${transcode_id}/360p/360p_segment%03d.ts" -start_number 0 outputs/${user_id}/${transcode_id}/360p/index.m3u8 -codec:v:1 libx264 -filter:v:1 scale=w=854:h=480 -b:v:1 1400k -maxrate:v:1 1498k -bufsize:v:1 2100k -crf 20 -g 48 -hls_time 10 -hls_playlist_type vod -hls_segment_filename "outputs/${user_id}/${transcode_id}/480p/480p_segment%03d.ts" -start_number 0 outputs/${user_id}/${transcode_id}/480p/index.m3u8 -codec:v:2 libx264 -filter:v:2 scale=w=1280:h=720 -b:v:2 2800k -maxrate:v:2 2996k -bufsize:v:2 4200k -crf 20 -g 48 -hls_time 10 -hls_playlist_type vod -hls_segment_filename "outputs/${user_id}/${transcode_id}/720p/720p_segment%03d.ts" -start_number 0 outputs/${user_id}/${transcode_id}/720p/index.m3u8 -codec:v:3 libx264 -filter:v:3 scale=w=1920:h=1080 -b:v:3 5000k -maxrate:v:3 5350k -bufsize:v:3 7500k -crf 20 -g 48 -hls_time 10 -hls_playlist_type vod -hls_segment_filename "outputs/${user_id}/${transcode_id}/1080p/1080p_segment%03d.ts" -start_number 0 outputs/${user_id}/${transcode_id}/1080p/index.m3u8`;
        const command = `mkdir outputs && cd outputs && mkdir ${user_id} && cd ${user_id} && mkdir ${transcode_id} && cd ${transcode_id} && mkdir 360p && mkdir 480p && mkdir 720p && mkdir 1080p && cd /home/app && cp master.m3u8 /home/app/outputs/${user_id}/${transcode_id} && ${ffmpegCommand}`;
        const p = exec(command);
        console.log("Executing script.js");
        await kafkaService.publishLog("Container Started...", fileMetaData);
        p.stdout.on("data", async function (data) {
          console.log(data.toString());
          await kafkaService.publishLog(data.toString(), fileMetaData);
        });

        p.stdout.on("error", async function (data) {
          console.log("Error", data.toString());
          await kafkaService.publishLog(
            `error: ${data.toString()}`,
            fileMetaData
          );

          // release occupied queue capacity in redis
          await releaseOccupiedSlot();
          process.exit(0);
        });

        p.on("close", async function () {
          const outputFolderPath = path.join(
            __dirname,
            `outputs/${user_id}/${transcode_id}`
          );

          const outputFolderContents = fs.readdirSync(outputFolderPath, {
            recursive: true,
          });
          
          // upload the files recursively into AWS S3
          for (const file of outputFolderContents) {
            const filePath = path.join(outputFolderPath, file);
            if (fs.lstatSync(filePath).isDirectory()) continue;

            console.log("uploading", filePath);
            await kafkaService.publishLog(`uploading ${file}`, fileMetaData);

            const command = s3Service.generatePutObjectCommand({
              Bucket: "uploadtranscoded",
              Key: `outputs/${user_id}/${transcode_id}/${file}`,
              Body: fs.createReadStream(filePath),
              ContentType: mime.lookup(filePath),
            });

            await s3Service.executeCommand(command);
            await kafkaService.publishLog(`uploaded ${file}`, fileMetaData);
            console.log("uploaded", filePath);
          }

          // release occupied queue capacity in redis
          await releaseOccupiedSlot(fileMetaData);

          // remove video from queue
          await redisService.removeVideoMetaDataFromRedisQueue(
            process.env.REDIS_QUEUE_NAME,
            FILE_KEY
          );

          // remove object from temporary bucket
          await removeFileFromTempBucket(fileMetaData);

          // remove file key from redis store
          await removeFileKeyFromRedis(fileMetaData);
          await kafkaService.publishLog(
            `FFmpeg command executed successfully`,
            fileMetaData
          );
          await mongoService.updateUserVideoCount(user_id)
          await kafkaService.publishLog(`Done`, fileMetaData);
          console.log("FFmpeg command executed successfully");
          process.exit(0);
        });
      } else {
        console.log("Key is not present in redis store.");
        process.exit(0);
      }
    } else {
      console.log("Redis client is not available at main().");
      process.exit(0);
    }
  } catch (error) {
    console.log("Problem connecting with redis at main().");
    process.exit(0);
  }
}

main();
