# deno-csv

CSV reader for https://deno.land/.

# Usage

## Reading CSV file

```ts
import { readCSV } from "https://deno.land/x/csv/mod.ts";

const f = await Deno.open("./example.csv");

for await (const row of readCSV(f)) {
  console.log(`line: ${row.join(' ')}`);
}

f.close();
```

## Reading CSV file with custom separators

```ts
import { readCSV } from "https://deno.land/x/csv/mod.ts";

const f = await Deno.open("./example.csv");

const options = {
  columnSeparator: new TextEncoder().encode(';'),
  lineSeparator: new TextEncoder().encode('\r\n'),
  quote: new TextEncoder().encode('$'),
};

for await (const row of readCSV(f, options)) {
  console.log(`line: ${row.join(' ')}`);
}

f.close();
```

## Reading objects from CSV file with header row

```ts
import { readCSVObjects } from "https://deno.land/x/csv/mod.ts";

const f = await Deno.open("./example.csv");

for await (const obj of readCSVObjects(f)) {
  console.log(obj);
}

f.close();
```
