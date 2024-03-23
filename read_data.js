const fs = require("fs");
const csv = require("csv-parser");

class ApplicationRecord {
  constructor(
    id,
    codeGender,
    flagOwnCar,
    flagOwnRealty,
    cntChildren,
    amtIncomeTotal,
    nameIncomeType,
    nameEducationType,
    nameFamilyStatus,
    nameHousingType,
    daysBirth,
    daysEmployed,
    flagMobil,
    flagWorkPhone,
    flagPhone,
    flagEmail,
    occupationType,
    cntFamMembers,
  ) {
    this.id = id;
    this.codeGender = codeGender;
    this.flagOwnCar = flagOwnCar;
    this.flagOwnRealty = flagOwnRealty;
    this.cntChildren = cntChildren;
    this.amtIncomeTotal = amtIncomeTotal;
    this.nameIncomeType = nameIncomeType;
    this.nameEducationType = nameEducationType;
    this.nameFamilyStatus = nameFamilyStatus;
    this.nameHousingType = nameHousingType;
    this.daysBirth = daysBirth;
    this.daysEmployed = daysEmployed;
    this.flagMobil = flagMobil;
    this.flagWorkPhone = flagWorkPhone;
    this.flagPhone = flagPhone;
    this.flagEmail = flagEmail;
    this.occupationType = occupationType;
    this.cntFamMembers = cntFamMembers;
  }
}

class CreditRecord {
  constructor(id, monthsBalance, status) {
    this.id = id;
    this.monthsBalance = monthsBalance;
    this.status = status;
  }
}

function parseRow(row) {
  // Convert numeric variables to their appropriate data types
  if (row["CNT_CHILDREN"]) row["CNT_CHILDREN"] = parseInt(row["CNT_CHILDREN"]);
  if (row["AMT_INCOME_TOTAL"])
    row["AMT_INCOME_TOTAL"] = parseFloat(row["AMT_INCOME_TOTAL"]);
  if (row["DAYS_BIRTH"]) row["DAYS_BIRTH"] = parseInt(row["DAYS_BIRTH"]);
  if (row["DAYS_EMPLOYED"])
    row["DAYS_EMPLOYED"] = parseInt(row["DAYS_EMPLOYED"]);
  if (row["CNT_FAM_MEMBERS"])
    row["CNT_FAM_MEMBERS"] = parseInt(row["CNT_FAM_MEMBERS"]);
  if (row["MONTHS_BALANCE"])
    row["MONTHS_BALANCE"] = parseInt(row["MONTHS_BALANCE"]);

  return row;
}

function readApplicationRecords() {
  return new Promise((resolve, reject) => {
    const applicationRecordPath = "./data/application_record.csv";
    const applicationRecords = [];
    fs.createReadStream(applicationRecordPath)
      .pipe(csv())
      .on("data", (row) => {
        row = parseRow(row);
        const applicationRecord = new ApplicationRecord(
          row["ID"],
          row["CODE_GENDER"],
          row["FLAG_OWN_CAR"],
          row["FLAG_OWN_REALTY"],
          row["CNT_CHILDREN"],
          row["AMT_INCOME_TOTAL"],
          row["NAME_INCOME_TYPE"],
          row["NAME_EDUCATION_TYPE"],
          row["NAME_FAMILY_STATUS"],
          row["NAME_HOUSING_TYPE"],
          row["DAYS_BIRTH"],
          row["DAYS_EMPLOYED"],
          row["FLAG_MOBIL"],
          row["FLAG_WORK_PHONE"],
          row["FLAG_PHONE"],
          row["FLAG_EMAIL"],
          row["OCCUPATION_TYPE"],
          row["CNT_FAM_MEMBERS"],
        );
        applicationRecords.push(applicationRecord);
      })
      .on("end", () => resolve(applicationRecords))
      .on("error", reject);
  });
}

function readCreditRecords() {
  return new Promise((resolve, reject) => {
    const creditRecordPath = "./data/credit_record.csv";
    const creditRecords = [];
    fs.createReadStream(creditRecordPath)
      .pipe(csv())
      .on("data", (row) => {
        row = parseRow(row);
        const creditRecord = new CreditRecord(
          row["ID"],
          row["MONTHS_BALANCE"],
          row["STATUS"],
        );
        creditRecords.push(creditRecord);
      })
      .on("end", () => resolve(creditRecords))
      .on("error", reject);
  });
}

module.exports = {
  readApplicationRecords,
  readCreditRecords,
};
