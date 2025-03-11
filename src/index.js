import express from "express";
import v1ApiRoutes from "./routes/v1/index.js";
import { runWorker } from "./services/worker.js";

const app = express();
app.use(express.json());

app.use("/api", v1ApiRoutes);

app.listen(3000, async () => {
  console.log("Server started on port 3000");
  await runWorker();
});
