import { concat } from "./deps.ts";
import { assertEquals, assertRejects } from "./dev_deps.ts";
import {
  readCSV,
  readCSVObjects,
  readCSVRows,
  readCSVStream,
} from "./reader.ts";
import { asyncArrayFrom, asyncArrayFrom2 } from "./utils.ts";

class MyReader implements Deno.Reader {
  private buf: Uint8Array;
  private index: number;

  constructor(content: string, options: { withBom?: boolean } = {}) {
    const opts = { withBom: false, ...options };
    this.buf = new TextEncoder().encode(content);
    if (opts.withBom) {
      this.buf = concat(new Uint8Array([0xef, 0xbb, 0xbf]), this.buf);
    }
    this.index = 0;
  }

  public read(p: Uint8Array): Promise<number | null> {
    const unread = this.buf.length - this.index;

    if (unread <= 0) {
      return Promise.resolve(null);
    }

    const toRead = Math.min(p.length, unread);

    p.set(this.buf.subarray(this.index, this.index + toRead));
    this.index += toRead;

    return Promise.resolve(toRead);
  }

  public reset() {
    this.index = 0;
  }
}

Deno.test({
  name: "readCSVObjects parses simple file",
  async fn() {
    const reader = new MyReader(`a,b,c
1,2,3`);

    const rows = await asyncArrayFrom(readCSVObjects(reader));

    assertEquals(rows, [{ a: "1", b: "2", c: "3" }]);
  },
});

Deno.test({
  name: "readCSV parses simple file",
  async fn() {
    const reader = new MyReader(`1,2,3
a,b,c`);

    const rows = await asyncArrayFrom2(readCSV(reader));

    assertEquals(rows, [
      ["1", "2", "3"],
      ["a", "b", "c"],
    ]);
  },
});

Deno.test({
  name: "readCSV parses file with UTF BOM",
  async fn() {
    const reader = new MyReader(`"1","2"`, { withBom: true });

    const rows = await asyncArrayFrom2(readCSV(reader));

    assertEquals(rows, [["1", "2"]]);
  },
});

Deno.test({
  name: "readCSV skips empty lines",
  async fn() {
    const reader = new MyReader(`1,2,3

a,b,c`);

    const rows = await asyncArrayFrom2(readCSV(reader));

    assertEquals(rows, [
      ["1", "2", "3"],
      ["a", "b", "c"],
    ]);
  },
});

Deno.test({
  name: "readCSV parses emoji",
  async fn() {
    const reader = new MyReader(`😀,2,3
a,😀,c`);

    const rows = await asyncArrayFrom2(readCSV(reader));

    assertEquals(rows, [
      ["😀", "2", "3"],
      ["a", "😀", "c"],
    ]);
  },
});

Deno.test({
  name: "readCSV parses file with quotes",
  async fn() {
    const reader = new MyReader(`1,"2",3
a,"b
""1",c`);

    const rows = await asyncArrayFrom2(readCSV(reader));

    assertEquals(rows, [
      ["1", "2", "3"],
      ["a", 'b\n"1', "c"],
    ]);
  },
});

Deno.test({
  name: "readCSV parses file with custom separators",
  async fn() {
    const reader = new MyReader(`a\tb\tc\r\n1\t2\t$$$3$`);

    const rows = await asyncArrayFrom2(
      readCSV(reader, {
        quote: "$",
        lineSeparator: new TextEncoder().encode("\r\n"),
        columnSeparator: "\t",
      }),
    );

    assertEquals(rows, [
      ["a", "b", "c"],
      ["1", "2", "$3"],
    ]);
  },
});

Deno.test({
  name: "readCSV throws when quote is unclosed",
  async fn() {
    const reader = new MyReader(`1,"2`);

    await assertRejects(
      async () => {
        await asyncArrayFrom2(readCSV(reader));
      },
      Error,
      "Expected quote, received EOF (line 1, character 5)",
    );
  },
});

Deno.test({
  name: "readCSV throws when quote is not last character in column",
  async fn() {
    const reader = new MyReader(`1,"2"3`);

    await assertRejects(
      async () => {
        await asyncArrayFrom2(readCSV(reader));
      },
      Error,
      "Expected EOF, COLUMN_SEPARATOR, LINE_SEPARATOR; received 3 (line 1, character 6)",
    );
  },
});

Deno.test({
  name: "readCSV throws when quote is found in unquoted field",
  async fn() {
    const reader = new MyReader(`1,2 "3",4`);

    await assertRejects(
      async () => {
        await asyncArrayFrom2(readCSV(reader));
      },
      Error,
      "Unexpected quote in unquoted field (line 1, character 5)",
    );
  },
});

Deno.test({
  name: "readCSV calculates error position",
  async fn() {
    const reader = new MyReader(`1,2
3,4

5,"123",,,"123


1"2
1,2`);

    await assertRejects(
      async () => {
        await asyncArrayFrom2(readCSV(reader));
      },
      Error,
      "Expected EOF, COLUMN_SEPARATOR, LINE_SEPARATOR; received 2 (line 7, character 3)",
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
    const reader = new MyReader(
      `aaaaaaaaaaaaaaaaaaaa,bbbbbbbbbbbbbbbbbbbbb\n11111111111111111111,22222222222222222222`,
    );

    const rows = await asyncArrayFrom2(
      readCSV(reader, {
        _readerIteratorBufferSize: 1,
        _columnBufferMinStepSize: 1,
        _inputBufferIndexLimit: 1,
        _columnBufferReserve: 1,
        _stats: stats,
        // deno-lint-ignore no-explicit-any
      } as any),
    );

    assertEquals(rows, [
      ["aaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbb"],
      ["11111111111111111111", "22222222222222222222"],
    ]);
    assertEquals(stats, {
      reads: 85,
      inputBufferShrinks: 84,
      columnBufferExpands: 11,
    });
  },
});

Deno.test({
  name: "readCSV read rows correctly even when rowsIterator not read",
  async fn() {
    const reader = new MyReader(`a,b\n1,2\n3,4`);

    let n = 0;
    for await (const _row of readCSV(reader)) {
      n++;
    }

    assertEquals(n, 3);
  },
});

Deno.test({
  name: "readCSVStream couldn't be used twice",
  async fn() {
    const reader = new MyReader(`a,b\n1,2\n3,4`);
    const r = readCSVStream(reader);

    let a = 0;
    for await (const _token of r) {
      a++;
    }
    reader.reset();
    let b = 0;
    for await (const _token of r) {
      b++;
    }

    assertEquals(a, 9);
    assertEquals(b, 0);
  },
});

Deno.test({
  name: "readCSVRows couldn't be used twice",
  async fn() {
    const reader = new MyReader(`a,b\n1,2\n3,4`);
    const r = readCSVRows(reader);

    let a = 0;
    for await (const _row of r) {
      a++;
    }
    reader.reset();
    let b = 0;
    for await (const _row of r) {
      b++;
    }

    assertEquals(a, 3);
    assertEquals(b, 0);
  },
});

Deno.test({
  name: "readCSV couldn't be used twice",
  async fn() {
    const reader = new MyReader(`a,b\n1,2\n3,4`);
    const r = readCSV(reader);

    let a = 0;
    for await (const _row of r) {
      a++;
    }
    reader.reset();
    let b = 0;
    for await (const _row of r) {
      b++;
    }

    assertEquals(a, 3);
    assertEquals(b, 0);
  },
});

Deno.test({
  name: "readCSVObjects couldn't be used twice",
  async fn() {
    const reader = new MyReader(`a,b\n1,2\n3,4`);
    const r = readCSVObjects(reader);

    let a = 0;
    for await (const _obj of r) {
      a++;
    }
    reader.reset();
    let b = 0;
    for await (const _obj of r) {
      b++;
    }

    assertEquals(a, 2);
    assertEquals(b, 0);
  },
});

Deno.test({
  name: "readCSVRows should work with long cell",
  async fn() {
    const reader = new MyReader(
      `"{""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false, ""key1"": false}"`,
    );

    let count = 0;
    for await (const _row of readCSVRows(reader)) {
      count++;
    }

    assertEquals(count, 1);
  },
});

Deno.test({
  name:
    "readCSVRows should skip lines if options.fromLine and options.toLine passed",
  async fn() {
    const reader = new MyReader(
      `a,b
c,d
e,f
g,h`,
    );

    const rows = await asyncArrayFrom(
      readCSVRows(reader, { fromLine: 1, toLine: 3 }),
    );

    assertEquals(rows, [
      ["c", "d"],
      ["e", "f"],
    ]);
  },
});

Deno.test({
  name: "readCSVRows options.toLine should be exclusive",
  async fn() {
    const reader = new MyReader(
      `a,b
c,d
e,f
g,h`,
    );

    const rows = await asyncArrayFrom(
      readCSVRows(reader, { fromLine: 1, toLine: 2 }),
    );

    assertEquals(rows, [["c", "d"]]);
  },
});

Deno.test({
  name: "readCSVRows can read only the first line",
  async fn() {
    const reader = new MyReader(
      `1,2,3
a,b,c
!,@,#`,
    );

    const rows = await asyncArrayFrom(
      readCSVRows(reader, {
        fromLine: 0,
        toLine: 1,
      }),
    );

    assertEquals(rows, [["1", "2", "3"]]);
  },
});

Deno.test({
  name: "readCSVRows can read empty lines (not prepends to the next line)",
  async fn() {
    const reader = new MyReader(
      `col1,col2,col3
a,b,c
,,
d,e,f`
    );
    const rows = await asyncArrayFrom(readCSVRows(reader));
    assertEquals(rows, [
      ["col1", "col2", "col3"],
      ["a", "b", "c"],
      ["", "", ""],
      ["d", "e", "f"]
    ])
  }
})
