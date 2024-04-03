const csvtojson = require("csvtojson");
const MongoClient = require("mongodb").MongoClient;

// application record
// ID,CODE_GENDER,FLAG_OWN_CAR,FLAG_OWN_REALTY,CNT_CHILDREN,AMT_INCOME_TOTAL,NAME_INCOME_TYPE,NAME_EDUCATION_TYPE,NAME_FAMILY_STATUS,NAME_HOUSING_TYPE,DAYS_BIRTH,DAYS_EMPLOYED,FLAG_MOBIL,FLAG_WORK_PHONE,FLAG_PHONE,FLAG_EMAIL,OCCUPATION_TYPE,CNT_FAM_MEMBERS
// credit record
// ID,MONTHS_BALANCE,STATUS

async function insert_mongo_data() {
  const url = process.env.MONGO_URL || "mongodb://localhost:27017";
  const client = new MongoClient(url);

  try {
    await client.connect();
    console.log("Connected correctly to server");

    const db = client.db("mydatabase");

    console.log("1");

    const applicationCollection = db.collection("applicationRecords");
    const creditCollection = db.collection("creditRecords");
    console.log("2");
    const applicationRecords = await csvtojson({
      checkType: true,
    }).fromFile("./data/application_record.csv");
    console.log("3");
    const creditRecords = await csvtojson({
      checkType: true,
    }).fromFile("./data/credit_record.csv");
    console.log("4");
    await applicationCollection.insertMany(applicationRecords);
    console.log("5");
    await creditCollection.insertMany(creditRecords);
    console.log("6");

    console.log("Data inserted successfully");

    // Add indexes after inserting the data
    await applicationCollection.createIndex({ ID: 1, CODE_GENDER: 1 });
    await creditCollection.createIndex({ ID: 1, STATUS: 1 });

    console.log("Indexes created successfully");
  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
  }
}

module.exports = { insert_mongo_data };
