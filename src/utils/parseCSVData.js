import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import csv from "csv-parser";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const parseCSVData = async () => {
  const stores = {};
  const filePath = path.resolve(
    __dirname,
    "../../data/StoreMasterAssignment.csv"
  );

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (row) => {
        stores[row.StoreID] = {
          storeName: row.StoreName,
          areaCode: row.AreaCode,
        };
      })
      .on("end", () => resolve(stores))
      .on("error", (err) => reject(err));
  });
};

export { parseCSVData };
