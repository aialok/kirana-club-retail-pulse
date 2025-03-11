import pool from "../client/dbClient.js";
import redisClient from "../client/redisClient.js";
import { imageQueue } from "../services/worker.js";

export const createJob = async (req, res) => {
  const { visits, counts } = req.body;

  if (counts !== visits.length || !visits || visits.length === 0) {
    return res.status(400).json({ error: "Invalid request" });
  }

  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();

    // Insert job
    const [jobResult] = await conn.query(
      "INSERT INTO jobs (status) VALUES ('pending')"
    );
    const job_id = jobResult.insertId;

    // Insert images
    for (const visit of visits) {
      for (const image_url of visit.image_url) {
        await conn.query(
          "INSERT INTO job_images (job_id, store_id, image_url, status) VALUES (?, ?, ?, 'pending')",
          [job_id, visit.store_id, image_url]
        );
      }
    }

    await conn.commit();
    await redisClient.set(`job_status:${job_id}`, "pending");
    await redisClient.expire(`job_status:${job_id}`, 3600); // Expire in 1 hour

    // Add job to BullMQ queue
    await imageQueue.add("processJob", { job_id });

    res.status(201).json({ job_id });
  } catch (error) {
    await conn.rollback();
    console.error(error);
    res.status(500).json({ error: "Internal Server Error" });
  } finally {
    conn.release();
  }
};

export const getJob = async (req, res) => {
  try {
    const { jobid } = req.query;

    if (!jobid) {
      return res.status(400).json({});
    }

    // Check Redis cache first
    let status = await redisClient.get(`job_status:${jobid}`);

    if (!status) {
      // Fetch status from MySQL if not in Redis
      const [rows] = await pool.query(
        "SELECT status FROM jobs WHERE job_id = ?",
        [jobid]
      );

      if (rows.length === 0) {
        return res.status(400).json({});
      }

      status = rows[0].status;
      await redisClient.setex(`job_status:${jobid}`, 3600, status); // Cache for 1 hour
    }

    let store_ids;
    if (status === "failed") {
      // Fetch store_id(s) associated with the failed job
      const [failedStores] = await pool.query(
        "SELECT DISTINCT store_id FROM job_images WHERE job_id = ? AND status = 'failed'",
        [jobid]
      );

      store_ids = failedStores.map((row) => row.store_id);
      return res.json({
        job_id: jobid,
        status: status,
        error: {
          store_ids: store_ids,
          error: "Store not found",
        },
      });
    }

    return res.json({
      job_id: jobid,
      status: status,
    });
  } catch (error) {
    console.error("Error fetching job status:", error);
    return res.status(500).json({ error: "Internal server error" });
  }
};
