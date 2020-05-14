import * as log from "https://deno.land/std/log/mod.ts";
import { readCSVObjects } from "./mod.ts";

await log.setup({
  handlers: {
    console: new log.handlers.ConsoleHandler("DEBUG"),
  },
  loggers: {
    csv: {
      level: "DEBUG",
      handlers: ["console"],
    },
  },
});

async function testFileReader(): Promise<Deno.Reader> {
  const buf = new Deno.Buffer();
  const enc = new TextEncoder();

  await buf.write(enc.encode('aa,"bb","cc"\n'));
  await buf.write(enc.encode('"1\n"",1",22,"33"\n\n'));
  await buf.write(enc.encode('"1\n"",1",22,"33"\n\n'));
  await buf.write(enc.encode('"1\n"",1",22,"33"\n\n'));

  return buf;
}

for await (const row of readCSVObjects(await testFileReader())) {
  console.log(row);
}
