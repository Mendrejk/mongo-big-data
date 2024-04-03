async function vintage_analysis(db) {
  const vintageAnalysis = await db
    .collection("applicationRecords")
    .aggregate([
      {
        $match: {
          CODE_GENDER: "M",
          DAYS_EMPLOYED: { $lt: -1095 },
        },
      },
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
        $group: {
          _id: {
            month: "$creditRecord.MONTHS_BALANCE",
            status: "$creditRecord.STATUS",
          },
          count: { $sum: 1 },
        },
      },
      {
        $sort: { "_id.month": 1 },
      },
    ])
    .toArray();

  // Calculate the cumulative number of bad customers for each cohort
  const badCustomers = vintageAnalysis.reduce((acc, curr) => {
    if (curr._id.status >= 4) {
      if (acc[curr._id.month]) {
        acc[curr._id.month] += curr.count;
      } else {
        acc[curr._id.month] = curr.count;
      }
    }
    return acc;
  }, {});

  // Calculate the cumulative number of bad customers, split by the number of days that are due
  const badCustomersSplitByDaysDue = vintageAnalysis.reduce((acc, curr) => {
    if (curr._id.status >= 1) {
      if (!acc[curr._id.month]) {
        acc[curr._id.month] = {
          "30+": 0,
          "60+": 0,
          "90+": 0,
          "120+": 0,
          "150+": 0,
        };
      }
      if (curr._id.status === 1) {
        acc[curr._id.month]["30+"] += curr.count;
      } else if (curr._id.status === 2) {
        acc[curr._id.month]["60+"] += curr.count;
      } else if (curr._id.status === 3) {
        acc[curr._id.month]["90+"] += curr.count;
      } else if (curr._id.status === 4) {
        acc[curr._id.month]["120+"] += curr.count;
      } else if (curr._id.status === 5) {
        acc[curr._id.month]["150+"] += curr.count;
      }
    }
    return acc;
  }, {});

  console.log(
    "\nCumulative number of bad customers for each cohort:",
    badCustomers,
    "\n",
  );
  console.log(
    "\nCumulative number of bad customers, split by the number of days that are due:",
    badCustomersSplitByDaysDue,
    "\n",
  );
}

module.exports = { vintage_analysis };
