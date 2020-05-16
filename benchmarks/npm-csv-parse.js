const csvParse = require("csv-parse");
const fs = require("fs");
const crypto = require("crypto");

const calculateHash = !!process.env.CALCULATE_HASH;
const calculateRowHash = !!process.env.CALCULATE_ROW_HASH;
const hash = crypto.createHash("sha256");

const file = fs.createReadStream(process.argv[2]);
const csvParser = csvParse();

let lines = 0;

if (calculateHash) {
  console.log('Calculating hash');
}

csvParser.on("readable", function () {
  let row;
  while ((row = csvParser.read())) {
    if (calculateHash) {
      for (const cell of row) {
        hash.update(cell);
      }
    }
    if (calculateRowHash) {
      const rowHash = crypto.createHash("sha256");
      for (const cell of row) {
        rowHash.update(cell);
      }
      console.log(lines, rowHash.digest("hex"), row);
    }
    lines++;
  }
});

csvParser.on("end", function () {
  const diff = process.hrtime(start);
  console.log(`Read ${lines} lines for ${diff[0]}.${diff[1]} seconds`);
  if (calculateHash) {
    console.log(`Result hash: ${hash.digest("hex")}`);
  }
});

const start = process.hrtime();
file.pipe(csvParser);
