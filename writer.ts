import { concat } from "@std/bytes/concat";
import { indexOfNeedle } from "@std/bytes/index-of-needle";
import type { SyncAsyncIterable } from "./utils.ts";
import {
  dummyAsyncIterable,
  getUint8Array,
  hasPrefixFrom,
  isAsyncIterable,
  makeAsyncIterable,
} from "./utils.ts";

/** Options for CSV writer */
export interface CSVWriterOptions {
  columnSeparator: string | Uint8Array;
  lineSeparator: string | Uint8Array;
  quote: string | Uint8Array;
}

/** Options for `CSVWriter.writeCell` */
export interface CSVWriteCellOptions {
  forceQuotes: boolean;
}

const defaultCSVWriterOptions = {
  columnSeparator: ",",
  lineSeparator: "\n",
  quote: '"',
};

/** Class for manual CSV writing:
 *
 *       const writer = new CSVWriter(f, {
 *         columnSeparator: "\t",
 *         lineSeparator: "\r\n",
 *       });
 *       await writer.writeCell("a\nb");
 *       await writer.nextLine();
 *       await writer.writeCell('1"2');
 */
export class CSVWriter {
  private writer: Deno.Writer;
  private columnSeparator: Uint8Array;
  private lineSeparator: Uint8Array;
  private quote: Uint8Array;
  private firstColumn: boolean;

  constructor(writer: Deno.Writer, options?: Partial<CSVWriterOptions>) {
    this.writer = writer;
    this.columnSeparator = getUint8Array(
      (options && options.columnSeparator) ||
        defaultCSVWriterOptions.columnSeparator,
    );
    this.lineSeparator = getUint8Array(
      (options && options.lineSeparator) ||
        defaultCSVWriterOptions.lineSeparator,
    );
    this.quote = getUint8Array(
      (options && options.quote) || defaultCSVWriterOptions.quote,
    );
    this.firstColumn = true;
  }

  public writeCell(
    str: string | Uint8Array | AsyncIterable<Uint8Array>,
    options?: Partial<CSVWriteCellOptions>,
  ): Promise<void> {
    if (isAsyncIterable(str)) {
      return this._writeCellAsyncIterable(str, { wrap: true });
    }

    const arr = getUint8Array(str);
    const wrap = options?.forceQuotes ||
      indexOfNeedle(arr, this.quote) >= 0 ||
      indexOfNeedle(arr, this.columnSeparator) >= 0 ||
      indexOfNeedle(arr, this.lineSeparator) >= 0;

    return this._writeCellAsyncIterable(dummyAsyncIterable(arr), {
      wrap,
    });
  }

  private async _writeCellAsyncIterable(
    iterable: AsyncIterable<Uint8Array>,
    options: { wrap: boolean },
  ): Promise<void> {
    const { quote } = this;
    const { wrap } = options;

    const iterator = iterable[Symbol.asyncIterator]();

    let inputBuffer = new Uint8Array();
    let inputBufferEmpty = false;
    let inputBufferIndex = 0;

    if (this.firstColumn) {
      this.firstColumn = false;
    } else {
      await this.writer.write(this.columnSeparator);
    }

    if (wrap) {
      await this.writer.write(this.quote);
    }

    while (true) {
      const inputBufferUnprocessed = inputBuffer.length - inputBufferIndex;

      if (inputBufferEmpty && inputBufferUnprocessed === 0) {
        break;
      }

      if (!inputBufferEmpty && inputBufferUnprocessed < quote.length) {
        const { done, value } = await iterator.next();
        if (done) {
          inputBufferEmpty = true;
        } else {
          inputBuffer = concat([inputBuffer, value]);
        }
        continue;
      }

      if (wrap && hasPrefixFrom(inputBuffer, quote, inputBufferIndex)) {
        await this.writer.write(quote);
        await this.writer.write(quote);
        inputBufferIndex += quote.length;
        continue;
      }

      if (inputBufferUnprocessed > 0) {
        await this.writer.write(
          inputBuffer.subarray(inputBufferIndex, inputBufferIndex + 1),
        );
        inputBufferIndex++;
        continue;
      }

      throw new Error("unexpected");
    }

    if (wrap) {
      await this.writer.write(this.quote);
    }
  }

  public async nextLine() {
    this.firstColumn = true;
    await this.writer.write(this.lineSeparator);
  }
}

/** Write CSV with sync or async row iterators:
 *
 *       await writeCSV(f, [["a", "b"], ["1", "2"]]);
 *
 *       const asyncRowGenerator = async function*() {
 *         yield ["a", "b"];
 *         yield ["1", "2"];
 *       }
 *       await writeCSV(f, asyncRowGenerator());
 */
export async function writeCSV(
  writer: Deno.Writer,
  iter: SyncAsyncIterable<
    SyncAsyncIterable<string | Uint8Array | AsyncIterable<Uint8Array>>
  >,
  options?: Partial<CSVWriterOptions & CSVWriteCellOptions>,
) {
  const csv = new CSVWriter(writer, options);

  let firstLine = true;

  for await (const row of makeAsyncIterable(iter)) {
    if (firstLine) {
      firstLine = false;
    } else {
      await csv.nextLine();
    }

    for await (const cell of makeAsyncIterable(row)) {
      await csv.writeCell(cell, options);
    }
  }
}

/** Write CSV with sync or async object iterators:
 *
 *       await writeCSVObjects(f, [{a: "1"}, {a: "2"}], { header: ["a"] });
 *
 *       const asyncObjectsGenerator = async function*() {
 *         yield { a: "1", b: "2", c: "3" };
 *         yield { a: "4", b: "5", c: "6" };
 *       }
 *       await writeCSVObjects(f, asyncObjectsGenerator(), { header: ["a", "b", "c"] });
 */
export async function writeCSVObjects(
  writer: Deno.Writer,
  iter: SyncAsyncIterable<{ [key: string]: string }>,
  options: Partial<CSVWriterOptions & CSVWriteCellOptions> & {
    header: string[];
  },
) {
  const { header } = options;

  const row = function* (obj: { [key: string]: string }) {
    for (const key of header) {
      yield obj[key];
    }
  };

  const rows = async function* () {
    yield header;

    for await (const obj of makeAsyncIterable(iter)) {
      yield row(obj);
    }
  };

  await writeCSV(writer, rows(), options);
}
