# deno-csv

> Streaming API for reading and writing CSV for https://deno.land/.

[![tag](https://img.shields.io/github/tag/vslinko/deno-csv.svg)](https://github.com/vslinko/deno-csv)
[![Build Status](https://github.com/vslinko/deno-csv/workflows/ci/badge.svg?branch=master)](https://github.com/vslinko/deno-csv/actions)
[![license](https://img.shields.io/github/license/vslinko/deno-csv.svg)](https://github.com/vslinko/deno-csv)
[![deno doc](https://doc.deno.land/badge.svg)](https://doc.deno.land/https/deno.land/x/csv/mod.ts)

## Usage

### Reading

#### Read CSV file

```ts
import { readCSV } from "https://deno.land/x/csv/mod.ts";

const f = await Deno.open("./example.csv");

for await (const row of readCSV(f)) {
  console.log("row:");
  for await (const cell of row) {
    console.log(`  cell: ${cell}`);
  }
}

f.close();
```

#### Read specific lines of CSV file

Line numbering starts from zero. `fromLine` is inclusive, `toLine` is exclusive.

```ts
import { readCSV } from "https://deno.land/x/csv/mod.ts";

const f = await Deno.open("./example.csv");

for await (const row of readCSV(f, { fromLine: 100, toLine: 200 })) {
  console.log("row:");
  for await (const cell of row) {
    console.log(`  cell: ${cell}`);
  }
}

f.close();
```

#### Read CSV file with custom separators

```ts
import { readCSV } from "https://deno.land/x/csv/mod.ts";

const f = await Deno.open("./example.csv");

const options = {
  columnSeparator: ";",
  lineSeparator: "\r\n",
  quote: "$",
};

for await (const row of readCSV(f, options)) {
  console.log("row:");
  for await (const cell of row) {
    console.log(`  cell: ${cell}`);
  }
}

f.close();
```

#### Read objects from CSV file with header row

```ts
import { readCSVObjects } from "https://deno.land/x/csv/mod.ts";

const f = await Deno.open("./example.csv");

for await (const obj of readCSVObjects(f)) {
  console.log(obj);
}

f.close();
```

#### Read CSV file manually

```ts
import { readCSVObjects } from "https://deno.land/x/csv/mod.ts";

const f = await Deno.open("./example.csv");

let row: string[] = [];
const reader = new CSVReader(f, {
  columnSeparator: "\t",
  lineSeparator: "\r\n",
  onCell(cell: string) {
    row.push(cell);
  },
  onRowEnd() {
    console.log(row);
    row = [];
  },
  onEnd() {
    console.log("end");
    f.close();
  },
  onError(err) {
    console.error(err);
  },
});
reader.read();
```

### Writing

#### Write CSV file

```ts
import { writeCSV } from "https://deno.land/x/csv/mod.ts";

const f = await Deno.open("./example.csv", {
  write: true,
  create: true,
  truncate: true,
});
const rows = [
  ["a", "b", "c"],
  ["1", "2", "3"],
];

await writeCSV(f, rows);

f.close();
```

#### Write objects asynchronously to CSV file

```ts
import { writeCSVObjects } from "https://deno.land/x/csv/mod.ts";

const f = await Deno.open("./example.csv", {
  write: true,
  create: true,
  truncate: true,
});
const header = ["a", "b", "c"];
const asyncObjectsGenerator = async function* () {
  yield { a: "1", b: "2", c: "3" };
  yield { a: "4", b: "5", c: "6" };
};

await writeCSVObjects(f, asyncObjectsGenerator(), { header });

f.close();
```

#### Write CSV file manually

```ts
import { CSVWriter } from "https://deno.land/x/csv/mod.ts";

const f = await Deno.open("./example.csv", {
  write: true,
  create: true,
  truncate: true,
});

const writer = new CSVWriter(f, {
  columnSeparator: "\t",
  lineSeparator: "\r\n",
});

await writer.writeCell("a");
await writer.writeCell("b");
await writer.writeCell("c");
await writer.nextLine();
await writer.writeCell("1");
await writer.writeCell("2");
await writer.writeCell("3");

f.close();
```

## Benchmarks

```
test-node-csv-parse
Read 500001 lines for 8.937 seconds
test-deno-csv-CSVReader
Read 500001 lines for 8.986 seconds
test-deno-csv-readCSVRows
Read 500001 lines for 9.425 seconds
test-deno-csv-readCSVStream
Read 500001 lines for 13.657 seconds
test-deno-csv-readCSV
Read 500001 lines for 15.814 seconds
```
