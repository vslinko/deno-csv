const {parse} = require("csv-parse");
const fs = require("fs");
const crypto = require("crypto");

const calculateHash = !!process.env.CALCULATE_HASH;
const calculateRowHash = !!process.env.CALCULATE_ROW_HASH;
const hash = crypto.createHash("sha256");

const file = fs.createReadStream(process.argv[2]);
const csvParser = parse();

let lines = 0;

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
  if (calculateHash) {
    console.log(`Result hash: ${hash.digest("hex")}`);
  } else {
    console.log(
      `Read ${lines} lines for ${diff[0]}.${Math.round(diff[1] / 1e6)} seconds`,
    );
  }
});

const start = process.hrtime();
file.pipe(csvParser);
