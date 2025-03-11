import { Queue } from "bullmq";
import redisClient from "./redisClient.js";

const imageQueue = new Queue("image-processing", { connection: redisClient });

export { imageQueue };
