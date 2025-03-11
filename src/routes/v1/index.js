import { Router } from "express";
import {
  createJob,
  getJob,
} from "../../controller/image-processing.controller.js";

const router = Router();


router.post("/submit", createJob);

router.get("/status/", getJob);

export default router;
