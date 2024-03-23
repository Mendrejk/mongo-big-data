const readData = require("./read_data");

async function main() {
  // zad. 1 - import zbiorów Kaggle
  const applicationRecords = await readData.readApplicationRecords();
  const creditRecords = await readData.readCreditRecords();
}

if (require.main === module) {
  main().then(() => process.exit(0));
}
