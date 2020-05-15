import { findIndex, concat } from "./deps.ts";
import {
  hasPrefixFrom,
  isAsyncIterable,
  dummyAsyncIterable,
  makeAsyncIterable,
  SyncAsyncIterable,
  getUint8Array,
} from "./utils.ts";

export interface CSVWriterOptions {
  columnSeparator: string | Uint8Array;
  lineSeparator: string | Uint8Array;
  quote: string | Uint8Array;
}

export interface CSVWriteCellOptions {
  forceQuotes: boolean;
}

const defaultCSVWriterOptions = {
  columnSeparator: ",",
  lineSeparator: "\n",
  quote: '"',
};

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

  public async writeCell(
    str: string | Uint8Array | AsyncIterable<Uint8Array>,
    options?: Partial<CSVWriteCellOptions>,
  ): Promise<void> {
    if (isAsyncIterable(str)) {
      return this._writeCellAsyncIterable(str, { wrap: true });
    }

    const arr = getUint8Array(str);
    const wrap = options?.forceQuotes ||
      findIndex(arr, this.quote) >= 0 ||
      findIndex(arr, this.columnSeparator) >= 0 ||
      findIndex(arr, this.lineSeparator) >= 0;

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
          inputBuffer = concat(inputBuffer, value);
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
