const MongoClient = require("mongodb").MongoClient;
const insert_mongo_data = require("./insert_mongo_data");

async function checkCollectionsExist(db, collectionNames) {
  const collections = await db.listCollections().toArray();
  const existingCollectionNames = collections.map((c) => c.name);
  return collectionNames.every((name) =>
    existingCollectionNames.includes(name),
  );
}

async function checkCollectionsHaveData(db, collectionNames) {
  for (const name of collectionNames) {
    const count = await db.collection(name).countDocuments();
    if (count === 0) {
      return false;
    }
  }
  return true;
}

async function main() {
  const url = process.env.MONGO_URL || "mongodb://localhost:27017";
  const client = new MongoClient(url);

  try {
    await client.connect();
    console.log("Connected correctly to server");

    const db = client.db("mydatabase");
    const collectionNames = ["applicationRecords", "creditRecords"];

    const collectionsExist = await checkCollectionsExist(db, collectionNames);
    const collectionsHaveData = await checkCollectionsHaveData(
      db,
      collectionNames,
    );

    if (!collectionsExist || !collectionsHaveData) {
      console.log("Collections not found or are empty. Inserting data...");
      await insert_mongo_data.insert_mongo_data();
    } else {
      console.log("Data is already present in the collections.");
    }
  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
  }
}

if (require.main === module) {
  main().then(() => process.exit(0));
}
