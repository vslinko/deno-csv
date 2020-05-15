import { findIndex, concat } from "./deps.ts";
import {
  hasPrefixFrom,
  isAsyncIterable,
  dummyAsyncIterable,
  makeAsyncIterable,
  SyncAsyncIterable,
} from "./utils.ts";

export interface CSVWriterOptions {
  columnSeparator: Uint8Array;
  lineSeparator: Uint8Array;
  quote: Uint8Array;
}

export interface CSVWriteCellOptions {
  forceQuotes: boolean;
}

const defaultCSVWriterOptions = {
  columnSeparator: new Uint8Array([44]), // ,
  lineSeparator: new Uint8Array([10]), // \n
  quote: new Uint8Array([34]), // "
};

export class CSVWriter {
  private writer: Deno.Writer;
  private encoder: TextEncoder;
  private columnSeparator: Uint8Array;
  private lineSeparator: Uint8Array;
  private quote: Uint8Array;
  private firstColumn: boolean;

  constructor(writer: Deno.Writer, options?: Partial<CSVWriterOptions>) {
    this.writer = writer;
    this.encoder = new TextEncoder();
    this.columnSeparator = (options && options.columnSeparator) ||
      defaultCSVWriterOptions.columnSeparator;
    this.lineSeparator = (options && options.lineSeparator) ||
      defaultCSVWriterOptions.lineSeparator;
    this.quote = (options && options.quote) || defaultCSVWriterOptions.quote;
    this.firstColumn = true;
  }

  public async writeCell(
    str: string | Uint8Array | AsyncIterable<Uint8Array>,
    options?: Partial<CSVWriteCellOptions>,
  ): Promise<void> {
    if (isAsyncIterable(str)) {
      return this._writeCellAsyncIterable(str, { wrap: true });
    }

    const arr = str instanceof Uint8Array ? str : this.encoder.encode(str);
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
  headers: string[],
  iter: SyncAsyncIterable<{ [key: string]: string }>,
  options?: Partial<CSVWriterOptions & CSVWriteCellOptions>,
) {
  const row = function* (obj: { [key: string]: string }) {
    for (const key of headers) {
      yield obj[key];
    }
  };

  const rows = async function* () {
    yield headers;

    for await (const obj of makeAsyncIterable(iter)) {
      yield row(obj);
    }
  };

  await writeCSV(writer, rows(), options);
}
