const csvtojson = require("csvtojson");
const MongoClient = require("mongodb").MongoClient;

async function insert_mongo_data() {
  const url = process.env.MONGO_URL || "mongodb://localhost:27017";
  const client = new MongoClient(url);

  try {
    await client.connect();
    console.log("Connected correctly to server");

    const db = client.db("mydatabase");

    const applicationCollection = db.collection("applicationRecords");
    const creditCollection = db.collection("creditRecords");

    const applicationRecords = await csvtojson({
      checkType: true,
    }).fromFile("./data/application_record.csv");
    const creditRecords = await csvtojson({
      checkType: true,
    }).fromFile("./data/credit_record.csv");

    await applicationCollection.insertMany(applicationRecords);
    await creditCollection.insertMany(creditRecords);

    console.log("Data inserted successfully");
  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
  }
}

module.exports = { insert_mongo_data };
