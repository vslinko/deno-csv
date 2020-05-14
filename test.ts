import {
  assertEquals,
  assertThrowsAsync,
} from "https://deno.land/std/testing/asserts.ts";
import { readCSV, readCSVObjects } from "./mod.ts";

async function createReader(content: string): Promise<Deno.Reader> {
  const buf = new Deno.Buffer();
  const enc = new TextEncoder();

  await buf.write(enc.encode(content));

  return buf;
}

async function asyncArrayFrom<T>(
  iter: AsyncIterableIterator<T>,
): Promise<Array<T>> {
  const arr: T[] = [];
  for await (const row of iter) {
    arr.push(row);
  }
  return arr;
}

Deno.test({
  name: "readCSVObjects parses simple file",
  async fn() {
    const reader = await createReader(`a,b,c
1,2,3`);

    const rows = await asyncArrayFrom(readCSVObjects(reader));

    assertEquals(rows, [{ a: "1", b: "2", c: "3" }]);
  },
});

Deno.test({
  name: "readCSV parses simple file",
  async fn() {
    const reader = await createReader(`1,2,3
a,b,c`);

    const rows = await asyncArrayFrom(readCSV(reader));

    assertEquals(rows, [
      ["1", "2", "3"],
      ["a", "b", "c"],
    ]);
  },
});

Deno.test({
  name: "readCSV skips empty lines",
  async fn() {
    const reader = await createReader(`1,2,3

a,b,c`);

    const rows = await asyncArrayFrom(readCSV(reader));

    assertEquals(rows, [
      ["1", "2", "3"],
      ["a", "b", "c"],
    ]);
  },
});

Deno.test({
  name: "readCSV parses emoji",
  async fn() {
    const reader = await createReader(`😀,2,3
a,😀,c`);

    const rows = await asyncArrayFrom(readCSV(reader));

    assertEquals(rows, [
      ["😀", "2", "3"],
      ["a", "😀", "c"],
    ]);
  },
});

Deno.test({
  name: "readCSV parses file with qoutes",
  async fn() {
    const reader = await createReader(`1,"2",3
a,"b
""1",c`);

    const rows = await asyncArrayFrom(readCSV(reader));

    assertEquals(rows, [
      ["1", "2", "3"],
      ["a", 'b\n"1', "c"],
    ]);
  },
});

Deno.test({
  name: "readCSV throws when qoute is unclosed",
  async fn() {
    const reader = await createReader(`1,"2`);

    assertThrowsAsync(
      async () => {
        await asyncArrayFrom(readCSV(reader));
      },
      Error,
      "Expected quote, received EOF",
    );
  },
});

Deno.test({
  name: "readCSV throws when qoute is not last character in column",
  async fn() {
    const reader = await createReader(`1,"2"3`);

    assertThrowsAsync(
      async () => {
        await asyncArrayFrom(readCSV(reader));
      },
      Error,
      "Expected EOF, COLUMN_SEPARATOR, LINE_SEPARATOR; received 3",
    );
  },
});

Deno.test({
  name: "readCSV parses huge file",
  async fn() {
    const stats = {
      reads: 0,
      inputBufferShrinks: 0,
      columnBufferExpands: 0,
    };
    const reader = await createReader(
      `aaaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbbbbbb\n11111111111111111111,22222222222222222222`,
    );

    const rows = await asyncArrayFrom(
      readCSV(reader, {
        _readerIteratorBufferSize: 1,
        _columnBufferMinStepSize: 1,
        _inputBufferIndexLimit: 1,
        _stats: stats,
      }),
    );

    assertEquals(rows, [
      ["aaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbb"],
      ["11111111111111111111", "22222222222222222222"],
    ]);
    assertEquals(stats, {
      reads: 85,
      inputBufferShrinks: 84,
      columnBufferExpands: 41,
    });
  },
});
