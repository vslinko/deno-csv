import { readCSV } from "../mod.ts";
import { SHA256 } from "chiefbiiko/sha256";

const file = await Deno.open(Deno.args[0]);
const calculateHash = !!Deno.env.get("CALCULATE_HASH");
const calculateRowHash = !!Deno.env.get("CALCULATE_ROW_HASH");
const hash = new SHA256();
let rowHash = new SHA256();
let lines = 0;

const start = performance.now();

for await (
  const row of readCSV(file, {
    lineSeparator: "\r\n",
  })
) {
  if (calculateRowHash) {
    rowHash = new SHA256();
  }
  for await (const cell of row) {
    if (calculateHash) {
      hash.update(cell);
    }
    if (calculateRowHash) {
      rowHash.update(cell);
    }
  }
  if (calculateRowHash) {
    console.log(lines, rowHash.digest("hex"), row);
  }
  lines++;
}

const diff = performance.now() - start;
if (calculateHash) {
  console.log(`Result hash: ${hash.digest("hex")}`);
} else {
  console.log(`Read ${lines} lines for ${(diff / 1000).toFixed(3)} seconds`);
}
