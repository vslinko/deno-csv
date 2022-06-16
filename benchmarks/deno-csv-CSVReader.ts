import { CSVReader } from "../mod.ts";
// import { Sha256 } from "https://deno.land/std@0.50.0/hash/sha256.ts";

const file = await Deno.open(Deno.args[0]);
const calculateHash = !!Deno.env.get("CALCULATE_HASH");
const calculateRowHash = !!Deno.env.get("CALCULATE_ROW_HASH");
// const hash = new Sha256();
let lines = 0;

const start = performance.now();

let row: string[] = [];

const reader = new CSVReader(file, {
  lineSeparator: "\r\n",
  onCell(cell: string) {
    if (calculateHash || calculateRowHash) {
      row.push(cell);
    }
  },
  onRowEnd() {
    // if (calculateHash) {
    //   for (const cell of row) {
    //     hash.update(cell);
    //   }
    // }
    // if (calculateRowHash) {
    //   const rowHash = new Sha256();
    //   for (const cell of row) {
    //     rowHash.update(cell);
    //   }
    //   console.log(lines, rowHash.hex(), row);
    // }
    // if (calculateHash || calculateRowHash) {
    //   row = [];
    // }
    lines++;
  },
  onEnd() {
    const diff = performance.now() - start;
    // if (calculateHash) {
    //   console.log(`Result hash: ${hash.hex()}`);
    // } else {
      console.log(
        `Read ${lines} lines for ${(diff / 1000).toFixed(3)} seconds`,
      );
    // }
  },
});

reader.read();
