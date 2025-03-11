import pkg from "bullmq";
const { Worker, Queue } = pkg;
import redisClient from "../client/redisClient.js";
import pool from "../client/dbClient.js";
import axios from "axios";
import { parseCSVData } from "../utils/parseCSVData.js";
import logger from "../utils/logger.js";

let storeData = {};

async function loadStoreData() {
  storeData = await parseCSVData();
  console.log("storeData", storeData);
}

const LOCK_DURATION = 300000;
const LOCK_RENEW_INTERVAL = 10000;
const lockRenewals = new Map();

const imageQueue = new Queue("image-processing", {
  connection: redisClient,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: "exponential", delay: 1000 },
    removeOnComplete: true,
    removeOnFail: 5,
  },
});

const worker = new Worker(
  "image-processing",
  async (job) => {
    if (!storeData || Object.keys(storeData).length === 0) {
      await loadStoreData(); 
    }

    const { job_id } = job.data;
    let renewalInterval = null;

    try {
      const renewLock = async () => {
        try {
          await job.extendLock(LOCK_DURATION);
          logger.info(`Lock extended for job ${job_id} (ID: ${job.id})`);
        } catch (err) {
          logger.error(
            `Failed to extend lock for job ${job_id} (ID: ${job.id}): ${err.message}`
          );
          try {
            await job.takeLock();
            logger.info(`Lock re-acquired for job ${job_id} (ID: ${job.id})`);
          } catch (lockErr) {
            logger.error(
              `Unable to re-acquire lock for job ${job_id} (ID: ${job.id}): ${lockErr.message}`
            );
          }
        }
      };

      renewalInterval = setInterval(renewLock, LOCK_RENEW_INTERVAL);
      lockRenewals.set(job.id, renewalInterval);

      const [[jobStatus]] = await pool.query(
        "SELECT status FROM jobs WHERE job_id = ?",
        [job_id]
      );

      if (jobStatus && jobStatus.status === "in_progress") {
        logger.info(
          `Job ${job_id} is already in progress. Verifying last update time.`
        );
        const [[lastUpdate]] = await pool.query(
          "SELECT TIMESTAMPDIFF(SECOND, updated_at, NOW()) as seconds_since_update FROM jobs WHERE job_id = ?",
          [job_id]
        );

        if (lastUpdate && lastUpdate.seconds_since_update > 300) {
          logger.info(
            `Job ${job_id} appears stalled (${lastUpdate.seconds_since_update}s). Resuming processing.`
          );
        } else {
          logger.info(
            `Job ${job_id} is currently being processed elsewhere. Aborting.`
          );
          return { skipped: true, reason: "already_in_progress" };
        }
      }

      await pool.query(
        "UPDATE jobs SET status = 'in_progress' WHERE job_id = ?",
        [job_id]
      );
      const [images] = (await pool.query(
        "SELECT * FROM job_images WHERE job_id = ?",
        [job_id]
      )) || [[]];
      logger.info(
        `Processing job ${job_id} (ID: ${job.id}). Number of images: ${images.length}`
      );

      let isAnyImageFailed = false;

      for (const image of images) {
        try {
          logger.info(`Processing image: ${image.image_url}`);
          const store_id = image.store_id;
          const store = storeData?.[store_id];
          console.log(store);

          if (!store) {
            await pool.query(
              'UPDATE job_images SET status = "failed" WHERE id = ?',
              [image.id]
            );
            await pool.query(
              'UPDATE jobs SET status = "failed" WHERE job_id = ?',
              [job_id]
            );
            await redisClient.set(`job_status:${job_id}`, "failed");
            await redisClient.expire(`job_status:${job_id}`, 3600);
            isAnyImageFailed = true;
            continue;
          }

          await job.extendLock(LOCK_DURATION);
          const imageData = await axios.head(image.image_url);
          const height = imageData.headers["content-length"];
          const perimeter = height * 2;
          await new Promise((r) => setTimeout(r, Math.random() * 300 + 100));
          await job.extendLock(LOCK_DURATION);
          await pool.query(
            "UPDATE job_images SET perimeter = ?, status = 'processed' WHERE id = ?",
            [perimeter, image.id]
          );
        } catch (error) {
          logger.error(
            `Error processing image ${image.image_url}: ${error.message}`
          );
          await pool.query(
            "UPDATE job_images SET status = 'failed' WHERE id = ?",
            [image.id]
          );
          await pool.query(
            "UPDATE jobs SET status = 'failed' WHERE job_id = ?",
            [job_id]
          );
          await redisClient.set(`job_status:${job_id}`, "failed");
          await redisClient.expire(`job_status:${job_id}`, 3600);
          isAnyImageFailed = true;
        }
      }
      console.log("isAnyImageFailed", isAnyImageFailed);
      if (isAnyImageFailed) {
        logger.info(`Job ${job_id} (ID: ${job.id}) failed.`);
        await pool.query("UPDATE jobs SET status = 'failed' WHERE job_id = ?", [
          job_id,
        ]);
        await redisClient.set(`job_status:${job_id}`, "failed");
        await redisClient.expire(`job_status:${job_id}`, 3600);
        return { success: false, processedImages: images.length };
      }

      await pool.query(
        "UPDATE jobs SET status = 'completed' WHERE job_id = ?",
        [job_id]
      );
      await redisClient.set(`job_status:${job_id}`, "completed");
      await redisClient.expire(`job_status:${job_id}`, 3600);
      logger.info(`Job ${job_id} (ID: ${job.id}) successfully completed.`);
      return { success: true, processedImages: images.length };
    } catch (error) {
      logger.error(
        `Job ${job_id} (ID: ${job.id}) encountered an error: ${error.message}`
      );
      throw error;
    } finally {
      if (renewalInterval) {
        clearInterval(renewalInterval);
        lockRenewals.delete(job.id);
        logger.info(`Lock renewal stopped for job ${job_id} (ID: ${job.id})`);
      }
    }
  },
  {
    connection: redisClient.options,
    concurrency: 2,
    removeOnComplete: true,
    removeOnFail: 5,
    lockDuration: LOCK_DURATION,
  }
);

worker.on("waiting", (jobId) =>
  logger.info(`Job ${jobId} is in queue, awaiting execution.`)
);
worker.on("completed", (job, result) =>
  logger.info(`Job ${job.id} completed successfully. Result:`, result)
);
worker.on("failed", (job, err) =>
  logger.error(`Job ${job.id} failed with error: ${err.message}`)
);
worker.on("stalled", (jobId) =>
  logger.warn(`Job ${jobId} has stalled and will be retried.`)
);
worker.on("error", (err) => logger.error("Worker encountered an error:", err));

const runWorker = async () => {
  logger.info("Worker started successfully.");
};

logger.info("Worker started successfully.");

export { runWorker, imageQueue, loadStoreData };
