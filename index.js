const { MongoClient } = require("mongodb");
const fs = require("fs").promises;
const path = require("path");

const uri = "mongodb://localhost:27017/wine";
const client = new MongoClient(uri);

async function main() {
  try {
    await client.connect();
    console.log("connected");
    const db = client.db();
    const results = await db.collection("tastings").find({}).count();
    if (results) {
      console.info("deleting collection");
      await db.collection("tastings").drop();
    }

    const data = await fs.readFile(path.join(__dirname, "wine.json"), "utf8");
    await db.collection("tastings").insertMany(JSON.parse(data));
    const count = await db.collection("tastings").find({}).count();
    console.info(`${count} records inserted into the wines collection`);
    process.exit();
  } catch (error) {
    console.error("could not insert data", error);
    process.exit();
  }
}

main();
