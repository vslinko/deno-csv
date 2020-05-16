import { readCSVRows } from "../mod.ts";
import { Sha256 } from "https://deno.land/std/hash/sha256.ts";

const file = await Deno.open(Deno.args[0]);
const calculateHash = !!Deno.env.get("CALCULATE_HASH");
const calculateRowHash = !!Deno.env.get("CALCULATE_ROW_HASH");
const hash = new Sha256();
let lines = 0;

if (calculateHash) {
  console.log("Calculating hash");
}

const start = performance.now();

for await (const row of readCSVRows(file, {
  lineSeparator: "\r\n",
})) {
  if (calculateHash) {
    for (const cell of row) {
      hash.update(cell);
    }
  }
  if (calculateRowHash) {
    const rowHash = new Sha256();
    for (const cell of row) {
      rowHash.update(cell);
    }
    console.log(lines, rowHash.hex(), row);
  }
  lines++;
}

const diff = performance.now() - start;
console.log(`Read ${lines} lines for ${diff / 1000} seconds`);
if (calculateHash) {
  console.log(`Result hash: ${hash.hex()}`);
}
