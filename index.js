const { MongoClient } = require("mongodb");
const fs = require("fs").promises;
const path = require("path");
const loading = require("loading-cli");

/**
 * constants
 */
const uri = "mongodb://localhost:27017/wine";
const client = new MongoClient(uri);

async function main() {
  try {
    await client.connect();
    const db = client.db();
    const results = await db.collection("tastings").find({}).count();

    /**
     * If existing records then delete the current collections
     */
    if (results) {
      console.info("deleting collection");
      await db.collection("tastings").drop();
      await db.collection("tasters").drop();
    }

    /**
     * This is just a fun little loader module that displays a spinner
     * to the command line
     */
    const load = loading("importing your wine!!").start();

    /**
     * Import the JSON data into the database
     */

    const data = await fs.readFile(path.join(__dirname, "wine.json"), "utf8");
    await db.collection("tastings").insertMany(JSON.parse(data));

    /**
     * This perhaps appears a little more complex than it is. Below, we are
     * grouping the wine tasters and summing their total tastings. Finally,
     * we tidy up the output so it represents the format we need for our new collection
     */

    const wineTastersRef = await db.collection("tastings").aggregate([
      { $match: { taster_name: { $ne: null } } },
      {
        $group: {
          _id: "$taster_name",
          social: { $push: "$taster_twitter_handle" },
          total_tastings: { $sum: 1 },
        },
      },
      {
        $project: {
          twitter: { $first: "$social" },
          tastings: "$total_tastings",
        },
      },
      { $set: { name: "$_id", _id: "$total_tastings" } },
    ]);
    /**
     * Below, we output the results of our aggregate into a
     * new collection
     */
    const wineTasters = await wineTastersRef.toArray();
    await db.collection("tasters").insertMany(wineTasters);

    /** Our final data manipulation is to reference each document in the
     * tastings collection to a taster id
     */

    const updatedWineTastersRef = db.collection("tasters").find({});
    const updatedWineTasters = await updatedWineTastersRef.toArray();
    updatedWineTasters.forEach(async ({ _id, name }) => {
      const result = await db
        .collection("tastings")
        .updateMany({ taster_name: name }, { $set: { taster_id: _id } });

      load.stop();
      console.info(
        `Wine collection set up! 🍷🍷🍷🍷🍷🍷🍷 \n
        I've also created a tasters collection for you 🥴 🥴 🥴`
      );
      process.exit();
    });
  } catch (error) {
    console.error("error:", error);
    process.exit();
  }
}

main();
