const MongoClient = require("mongodb").MongoClient;
const insert_mongo_data = require("./insert_mongo_data");
const vintage_analysis = require("./vintage_analysis");

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

async function checkAndInsertData(client) {
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
}

async function exercise_2a(db) {
  const womenWithCarsAndRealtyNoChildrenSingle = await db
    .collection("applicationRecords")
    .find({
      CODE_GENDER: "F",
      FLAG_OWN_CAR: "Y",
      FLAG_OWN_REALTY: "Y",
      CNT_CHILDREN: 0,
      NAME_FAMILY_STATUS: { $in: ["Single / not married"] },
    })
    .toArray();
  console.log(
    "\nWomen with cars and realty, no children, single/not married:",
    womenWithCarsAndRealtyNoChildrenSingle,
    "\n",
  );

  const incomeStatsByEmploymentType = await db
    .collection("applicationRecords")
    .aggregate([
      {
        $group: {
          _id: "$NAME_INCOME_TYPE",
          minIncome: { $min: "$AMT_INCOME_TOTAL" },
          maxIncome: { $max: "$AMT_INCOME_TOTAL" },
          avgIncome: { $avg: "$AMT_INCOME_TOTAL" },
        },
      },
    ])
    .toArray();
  console.log(
    "\nIncome stats by employment type:",
    incomeStatsByEmploymentType,
    "\n",
  );

  const incomeStatsByEducationType = await db
    .collection("applicationRecords")
    .aggregate([
      {
        $group: {
          _id: "$NAME_EDUCATION_TYPE",
          minIncome: { $min: "$AMT_INCOME_TOTAL" },
          maxIncome: { $max: "$AMT_INCOME_TOTAL" },
          avgIncome: { $avg: "$AMT_INCOME_TOTAL" },
        },
      },
    ])
    .toArray();
  console.log(
    "\nIncome stats by education type:",
    incomeStatsByEducationType,
    "\n",
  );

  const incomeStatsByEmploymentAndEducationType = await db
    .collection("applicationRecords")
    .aggregate([
      {
        $group: {
          _id: {
            employmentType: "$NAME_INCOME_TYPE",
            educationType: "$NAME_EDUCATION_TYPE",
          },
          minIncome: { $min: "$AMT_INCOME_TOTAL" },
          maxIncome: { $max: "$AMT_INCOME_TOTAL" },
          avgIncome: { $avg: "$AMT_INCOME_TOTAL" },
        },
      },
    ])
    .toArray();
  console.log(
    "\nIncome stats by employment and education type:",
    incomeStatsByEmploymentAndEducationType,
    "\n",
  );
}

async function exercise_2b(db) {
  const incomeStats = await db
    .collection("applicationRecords")
    .aggregate([
      {
        $facet: {
          incomeStatsByEmploymentType: [
            {
              $group: {
                _id: "$NAME_INCOME_TYPE",
                minIncome: { $min: "$AMT_INCOME_TOTAL" },
                maxIncome: { $max: "$AMT_INCOME_TOTAL" },
                avgIncome: { $avg: "$AMT_INCOME_TOTAL" },
              },
            },
          ],
          incomeStatsByEducationType: [
            {
              $group: {
                _id: "$NAME_EDUCATION_TYPE",
                minIncome: { $min: "$AMT_INCOME_TOTAL" },
                maxIncome: { $max: "$AMT_INCOME_TOTAL" },
                avgIncome: { $avg: "$AMT_INCOME_TOTAL" },
              },
            },
          ],
          incomeStatsByEmploymentAndEducationType: [
            {
              $group: {
                _id: {
                  employmentType: "$NAME_INCOME_TYPE",
                  educationType: "$NAME_EDUCATION_TYPE",
                },
                minIncome: { $min: "$AMT_INCOME_TOTAL" },
                maxIncome: { $max: "$AMT_INCOME_TOTAL" },
                avgIncome: { $avg: "$AMT_INCOME_TOTAL" },
              },
            },
          ],
        },
      },
    ])
    .toArray();

  console.log(
    "\nIncome stats by employment type:",
    incomeStats[0].incomeStatsByEmploymentType,
    "\n",
  );
  console.log(
    "\nIncome stats by education type:",
    incomeStats[0].incomeStatsByEducationType,
    "\n",
  );
  console.log(
    "\nIncome stats by employment and education type:",
    incomeStats[0].incomeStatsByEmploymentAndEducationType,
    "\n",
  );
}

// bez indeksów na ID, CODE_GENDER i STATUS zapytanie trwało bardzo długo
async function exercise_3(db) {
  const womenWhoDidNotDefault = await db
    .collection("applicationRecords")
    .aggregate([
      {
        $lookup: {
          from: "creditRecords",
          let: { applicationRecordId: "$ID" },
          pipeline: [
            {
              $match: {
                $expr: { $eq: ["$ID", "$$applicationRecordId"] },
                STATUS: { $in: ["C", "X"] },
              },
            },
          ],
          as: "creditRecord",
        },
      },
      {
        $match: {
          CODE_GENDER: "F",
          "creditRecord.0": { $exists: true },
        },
      },
      {
        $count: "count",
      },
    ])
    .toArray();

  console.log(
    "\nNumber of women who did not default on their credit in the previous month:",
    womenWhoDidNotDefault[0]?.count || 0,
    "\n",
  );
}

async function exercise_4(db) {
  const peopleWithDelays = await db
    .collection("applicationRecords")
    .aggregate([
      {
        $lookup: {
          from: "creditRecords",
          localField: "ID",
          foreignField: "ID",
          as: "creditRecord",
        },
      },
      {
        $unwind: "$creditRecord",
      },
      {
        $match: {
          "creditRecord.STATUS": 1,
        },
      },
      {
        $group: {
          _id: { ID: "$ID", NAME_INCOME_TYPE: "$NAME_INCOME_TYPE" },
          count: { $sum: 1 },
        },
      },
      {
        $match: {
          count: { $gt: 2 },
        },
      },
      {
        $group: {
          _id: "$_id.NAME_INCOME_TYPE",
          count: { $sum: 1 },
        },
      },
    ])
    .toArray();

  console.log(
    "\nNumber of people with more than 2 months of 30-59 days delays, grouped by income type:",
    peopleWithDelays,
    "\n",
  );
}

async function exercise_5(db) {
  const avgIncomeByBirthYear = await db
    .collection("applicationRecords")
    .aggregate([
      {
        $addFields: {
          BIRTH_YEAR: {
            $subtract: [2020, { $divide: ["$DAYS_BIRTH", -365.25] }],
          },
        },
      },
      {
        $group: {
          _id: { $toInt: "$BIRTH_YEAR" },
          avgIncome: { $avg: "$AMT_INCOME_TOTAL" },
        },
      },
      {
        $sort: { _id: 1 },
      },
    ])
    .toArray();

  console.log("\nAverage income by birth year:", avgIncomeByBirthYear, "\n");
}

async function exercise_6(db) {
  const avgDelayedPeopleByEmploymentLength = await db
    .collection("applicationRecords")
    .aggregate([
      {
        $lookup: {
          from: "creditRecords",
          localField: "ID",
          foreignField: "ID",
          as: "creditRecord",
        },
      },
      {
        $unwind: "$creditRecord",
      },
      {
        $match: {
          "creditRecord.MONTHS_BALANCE": -1,
          "creditRecord.STATUS": { $gte: 1, $lte: 5 },
        },
      },
      {
        $addFields: {
          EMPLOYMENT_LENGTH: {
            $divide: ["$DAYS_EMPLOYED", -365],
          },
        },
      },
      {
        $bucket: {
          groupBy: "$EMPLOYMENT_LENGTH",
          boundaries: [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50],
          default: "50+",
          output: {
            count: { $sum: 1 },
            avgDelayedPeople: { $avg: 1 },
          },
        },
      },
    ])
    .toArray();

  console.log(
    "\nAverage number of people delayed with credit repayment in the last month by employment length:",
    avgDelayedPeopleByEmploymentLength,
    "\n",
  );
}

async function main() {
  const url = process.env.MONGO_URL || "mongodb://localhost:27017";
  const client = new MongoClient(url);

  try {
    await checkAndInsertData(client);
    const db = client.db("mydatabase");
    // await exercise_2a(db);
    // await exercise_2b(db);
    // await exercise_3(db);
    // await exercise_4(db);
    // await exercise_5(db);
    // await exercise_6(db);
    await vintage_analysis.vintage_analysis(db);
  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
  }
}

if (require.main === module) {
  main().then(() => process.exit(0));
}
