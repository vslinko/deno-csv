import { readCSVObjects } from "./mod.ts";
import { assertEquals } from "@std/assert/equals";

for await (const file of Deno.readDir("./third_party/csv-spectrum/csvs")) {
  const csvFilePath = "./third_party/csv-spectrum/csvs/" + file.name;
  const jsonFilePath = csvFilePath
    .replace(/\/csvs\//, "/json/")
    .replace(/\.csv$/, ".json");

  Deno.test({
    name: csvFilePath,
    async fn() {
      const csvFile = await Deno.open(csvFilePath);
      const expectedJSON = JSON.parse(await Deno.readTextFile(jsonFilePath));
      const actualJSON = [];
      const options = {
        lineSeparator: csvFilePath.includes("crlf") ? "\r\n" : "\n",
      };

      for await (const row of readCSVObjects(csvFile, options)) {
        actualJSON.push(row);
      }

      assertEquals(actualJSON, expectedJSON);

      csvFile.close();
    },
  });
}
