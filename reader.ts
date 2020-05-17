import { repeat, concat, getLogger, Logger } from "./deps.ts";
import { hasPrefixFrom, getUint8Array } from "./utils.ts";

/** Common options for CSV reader module */
export interface CommonCSVReaderOptions {
  columnSeparator: string | Uint8Array;
  lineSeparator: string | Uint8Array;
  quote: string | Uint8Array;
}

/** Options for CSVReader class */
export interface CSVReaderOptions extends CommonCSVReaderOptions {
  onCell(cell: string): void;
  onRowEnd(): void;
  onEnd(): void;
  onError(err: Error): void;
}

interface HiddenCSVReaderOptions extends CSVReaderOptions {
  _readerIteratorBufferSize: number;
  _columnBufferMinStepSize: number;
  _inputBufferIndexLimit: number;
  _stats: {
    reads: number;
    inputBufferShrinks: number;
    columnBufferExpands: number;
  };
}

function noop(a?: any): any {}

const defaultCSVReaderOptions: HiddenCSVReaderOptions = {
  columnSeparator: ",",
  lineSeparator: "\n",
  quote: '"',
  onCell: noop,
  onRowEnd: noop,
  onEnd: noop,
  onError: noop,
  _readerIteratorBufferSize: 1024 * 1024,
  _columnBufferMinStepSize: 1024,
  _inputBufferIndexLimit: 1024,
  _stats: {
    reads: 0,
    inputBufferShrinks: 0,
    columnBufferExpands: 0,
  },
};

/** Class for manual CSV reading:
 *
 *       let row: string[] = [];
 *       const reader = new CSVReader(f, {
 *         columnSeparator: "\t",
 *         lineSeparator: "\r\n",
 *         onCell(cell: string) {
 *           row.push(cell);
 *         },
 *         onRowEnd() {
 *           console.log(row);
 *           row = [];
 *         },
 *         onEnd() {
 *           console.log('end');
 *         },
 *         onError(err) {
 *           console.error(err);
 *         }
 *       });
 *       reader.read();
 */
export class CSVReader {
  private decoder: TextDecoder;
  private onCell: (cell: string) => void;
  private onRowEnd: () => void;
  private onEnd: () => void;
  private onError: (err: Error) => void;
  private _readerIteratorBufferSize: number;
  private _columnBufferMinStepSize: number;
  private _inputBufferIndexLimit: number;
  private _stats: {
    reads: number;
    inputBufferShrinks: number;
    columnBufferExpands: number;
  };
  private columnSeparator: Uint8Array;
  private lineSeparator: Uint8Array;
  private quote: Uint8Array;
  private doubleQuote: Uint8Array;
  private minPossibleBufferReserve: number;
  private columnBufferStepSize: number;
  private readerIterator: AsyncIterableIterator<Uint8Array>;
  private inputBuffer: Uint8Array;
  private inputBufferIndex: number;
  private columnBuffer: Uint8Array;
  private columnBufferIndex: number;
  private readerEmpty: boolean;
  private emptyLine: boolean;
  private inQuote: boolean;
  private inColumn: boolean;
  private inputBufferUnprocessed: number;
  private paused: boolean;
  private debug: (msg: string) => void;

  constructor(reader: Deno.Reader, options?: Partial<CSVReaderOptions>) {
    this.decoder = new TextDecoder();
    const mergedOptions: HiddenCSVReaderOptions = {
      ...defaultCSVReaderOptions,
      ...options,
    };
    this.onCell = mergedOptions.onCell || noop;
    this.onRowEnd = mergedOptions.onRowEnd || noop;
    this.onEnd = mergedOptions.onEnd || noop;
    this.onError = mergedOptions.onError || noop;
    this._readerIteratorBufferSize = mergedOptions._readerIteratorBufferSize;
    this._columnBufferMinStepSize = mergedOptions._columnBufferMinStepSize;
    this._inputBufferIndexLimit = mergedOptions._inputBufferIndexLimit;
    this._stats = mergedOptions._stats;
    this.quote = getUint8Array(mergedOptions.quote);
    this.columnSeparator = getUint8Array(mergedOptions.columnSeparator);
    this.lineSeparator = getUint8Array(mergedOptions.lineSeparator);
    this.doubleQuote = repeat(this.quote, 2);

    this.minPossibleBufferReserve = Math.max(
      this.columnSeparator.length,
      this.lineSeparator.length,
      this.doubleQuote.length,
      1,
    );
    this.columnBufferStepSize = Math.max(
      this._columnBufferMinStepSize,
      this.minPossibleBufferReserve,
    );

    this.readerIterator = Deno.iter(reader, {
      bufSize: this._readerIteratorBufferSize,
    });

    this.inputBuffer = new Uint8Array();
    this.inputBufferIndex = 0;

    this.columnBuffer = new Uint8Array(this.columnBufferStepSize);
    this.columnBufferIndex = 0;

    this.readerEmpty = false;
    this.emptyLine = true;
    this.inQuote = false;
    this.inColumn = false;
    this.inputBufferUnprocessed = 0;
    this.paused = true;

    const logger: Logger = getLogger("csv");
    if (logger.levelName === "DEBUG") {
      this.debug = (msg) => logger.debug(msg);
    } else {
      this.debug = noop;
    }
  }

  public read() {
    if (this.paused) {
      this.paused = false;
      this.parseCycle();
    }
  }

  public pause() {
    this.paused = true;
  }

  private processColumn() {
    const result = this.decoder.decode(
      this.columnBuffer.subarray(0, this.columnBufferIndex),
    );
    this.columnBufferIndex = 0;
    this.onCell(result);
  }

  private processRow() {
    this.onRowEnd();
  }

  private hasNext(chars: Uint8Array) {
    return hasPrefixFrom(this.inputBuffer, chars, this.inputBufferIndex);
  }

  private skip(chars: Uint8Array) {
    this.inputBufferIndex += chars.length;
    this.inputBufferUnprocessed -= chars.length;
  }

  private shrinkInputBuffer() {
    this._stats.inputBufferShrinks++;
    this.debug("shrink input buffer");
    this.inputBuffer = this.inputBuffer.slice(this.inputBufferIndex);
    this.inputBufferIndex = 0;
    this.inputBufferUnprocessed = this.inputBuffer.length;
  }

  private readChars(n: number) {
    this.columnBuffer.set(
      this.inputBuffer.subarray(
        this.inputBufferIndex,
        this.inputBufferIndex + n,
      ),
      this.columnBufferIndex,
    );
    this.columnBufferIndex += n;
    this.inputBufferIndex += n;
    this.inputBufferUnprocessed -= n;
  }

  private async readMoreData() {
    this._stats.reads++;
    this.debug("read more data");
    const { done, value } = await this.readerIterator.next();
    if (done) {
      this.readerEmpty = true;
    } else {
      this.inputBuffer = concat(this.inputBuffer, value);
      this.inputBufferUnprocessed += value.length;
    }
  }

  private expandColumnBuffer() {
    this._stats.columnBufferExpands++;
    const newColumn = new Uint8Array(
      this.columnBuffer.length + this.columnBufferStepSize,
    );
    this.debug(
      `expand column buffer from ${this.columnBuffer.length} to ${newColumn.length}`,
    );
    newColumn.set(this.columnBuffer);
    this.columnBuffer = newColumn;
  }

  private async parseCycle() {
    while (true) {
      if (this.paused) {
        return;
      }

      // lacks of data
      if (
        !this.readerEmpty &&
        this.inputBufferUnprocessed < this.minPossibleBufferReserve
      ) {
        // TODO: many loops could be started if pause/unpause while reading
        await this.readMoreData();
        continue;
      }

      // buffer could be emptied
      if (this.inputBufferIndex >= this._inputBufferIndexLimit) {
        this.shrinkInputBuffer();
        continue;
      }

      // column buffer is almost full
      if (
        this.columnBuffer.length - this.columnBufferIndex <
          this.minPossibleBufferReserve
      ) {
        this.expandColumnBuffer();
        continue;
      }

      if (!this.inColumn && this.inputBufferUnprocessed === 0) {
        this.debug("eof");
        if (!this.emptyLine) {
          this.processColumn();
          this.processRow();
        }
        this.onEnd();
        return;
      }

      if (!this.inColumn && this.hasNext(this.lineSeparator)) {
        this.debug("lineSeparator");
        if (!this.emptyLine) {
          this.processColumn();
          this.processRow();
        }
        this.skip(this.lineSeparator);
        this.emptyLine = true;
        continue;
      }

      if (!this.inColumn && this.hasNext(this.columnSeparator)) {
        this.debug("columnSeparator");
        this.processColumn();
        this.skip(this.columnSeparator);
        continue;
      }

      if (!this.inColumn) {
        this.inColumn = true;
        this.emptyLine = false;
        if (this.hasNext(this.quote)) {
          this.debug("start quoted column");
          this.inQuote = true;
          this.skip(this.quote);
        } else {
          this.debug("start unquoted column");
        }
        continue;
      }

      if (this.inColumn && this.inQuote && this.hasNext(this.doubleQuote)) {
        this.debug("double quote");
        this.columnBuffer.set(this.quote, this.columnBufferIndex);
        this.columnBufferIndex += this.quote.length;
        this.skip(this.doubleQuote);
        continue;
      }

      if (this.inColumn && this.inQuote && this.hasNext(this.quote)) {
        this.debug("end quoted column");
        this.inQuote = false;
        this.inColumn = false;
        this.skip(this.quote);
        if (
          this.inputBufferUnprocessed > 0 &&
          !this.hasNext(this.lineSeparator) &&
          !this.hasNext(this.columnSeparator)
        ) {
          const char = String.fromCharCode(
            this.inputBuffer[this.inputBufferIndex],
          );
          this.onError(
            new Error(
              `Expected EOF, COLUMN_SEPARATOR, LINE_SEPARATOR; received ${char}`,
            ),
          );
          return;
        }
        continue;
      }

      if (
        this.inColumn &&
        !this.inQuote &&
        (this.inputBufferUnprocessed === 0 ||
          this.hasNext(this.lineSeparator) ||
          this.hasNext(this.columnSeparator))
      ) {
        this.debug("end unquoted column");
        this.inColumn = false;
        continue;
      }

      if (this.inColumn && this.inputBufferUnprocessed > 0) {
        const slice = this.inputBuffer.subarray(this.inputBufferIndex);
        const limit = slice.length - this.minPossibleBufferReserve;
        const readTillIndex = limit <= 1
          ? 1
          : this.inQuote
          ? findReadTillIndex(slice, limit, this.quote)
          : findReadTillIndex3(
            slice,
            limit,
            this.lineSeparator,
            this.columnSeparator,
            this.quote,
          );

        if (readTillIndex > 0) {
          this.debug(`read char: ${readTillIndex}`);
          this.readChars(readTillIndex);
        }
        continue;
      }

      if (this.inQuote && this.inputBufferUnprocessed === 0) {
        this.onError(new Error("Expected quote, received EOF"));
        return;
      }

      this.onError(new Error("unexpected"));
      return;
    }
  }
}

class CSVStreamReader implements AsyncIterableIterator<string | symbol> {
  private reader: CSVReader;
  private done: boolean;
  private buffer: Array<IteratorResult<string | symbol, void> | Error>;
  private nextPromise?: Promise<IteratorResult<string | symbol, void>>;
  private nextPromiseResolve?: (
    res: IteratorResult<string | symbol, void>,
  ) => void;
  private nextPromiseReject?: (err: Error) => void;

  constructor(reader: Deno.Reader, options?: Partial<CommonCSVReaderOptions>) {
    this.buffer = [];
    this.done = false;
    this.reader = new CSVReader(reader, {
      ...options,
      onCell: (value) => this.onCell(value),
      onRowEnd: () => this.onRowEnd(),
      onEnd: () => this.onEnd(),
      onError: (err) => this.onError(err),
    });
  }

  private onCell(value: string) {
    this.process({ done: false, value });
  }

  private onRowEnd() {
    this.process({ done: false, value: newLine });
  }

  private onEnd() {
    this.done = true;
    this.process({ done: true, value: undefined });
  }

  private onError(err: Error) {
    this.process(err);
  }

  private process(result: IteratorResult<string | symbol, void> | Error) {
    const cb = result instanceof Error
      ? this.nextPromiseReject
      : this.nextPromiseResolve;

    if (cb) {
      this.nextPromise = undefined;
      this.nextPromiseResolve = undefined;
      this.nextPromiseReject = undefined;
      cb(result as any);
    } else {
      this.buffer.push(result);
    }

    this.reader.pause();
  }

  next(): Promise<IteratorResult<string | symbol, void>> {
    if (this.done && this.buffer.length === 0) {
      return Promise.resolve({ done: true, value: undefined });
    }

    let promise = this.nextPromise;
    if (!promise) {
      if (this.buffer.length > 0) {
        const res = this.buffer.shift()!;
        if (res instanceof Error) {
          return Promise.reject(res);
        } else {
          return Promise.resolve(res);
        }
      }

      promise = new Promise((resolve, reject) => {
        this.nextPromiseResolve = resolve;
        this.nextPromiseReject = reject;
        this.reader.read();
      });
    }

    // not resolved yet
    if (this.nextPromiseResolve) {
      this.nextPromise = promise;
    }

    return promise;
  }

  [Symbol.asyncIterator]() {
    return this;
  }
}

/** readCSVStream returns this symbol to show that row is ended */
export const newLine = Symbol("newLine");

/** Read CSV as stream of cells and newlines:
 *
 *       for await (const token of readCSVStream(f)) {
 *         if (token === newLine) {
 *           console.log('new line');
 *         } else {
 *           console.log(`cell: ${token}`);
 *         }
 *       }
 */
export function readCSVStream(
  reader: Deno.Reader,
  options?: Partial<CommonCSVReaderOptions>,
): AsyncIterable<string | symbol> {
  return new CSVStreamReader(reader, options);
}

class CSVRowReader implements AsyncIterableIterator<string[]> {
  private reader: CSVReader;
  private done: boolean;
  private row: string[];
  private buffer: Array<IteratorResult<string[], void> | Error>;
  private nextPromise?: Promise<IteratorResult<string[], void>>;
  private nextPromiseResolve?: (res: IteratorResult<string[], void>) => void;
  private nextPromiseReject?: (err: Error) => void;

  constructor(reader: Deno.Reader, options?: Partial<CommonCSVReaderOptions>) {
    this.buffer = [];
    this.done = false;
    this.row = [];
    this.reader = new CSVReader(reader, {
      ...options,
      onCell: (value) => this.onCell(value),
      onRowEnd: () => this.onRowEnd(),
      onEnd: () => this.onEnd(),
      onError: (err) => this.process(err),
    });
  }

  private onCell(cell: string) {
    this.row.push(cell);
  }

  private onRowEnd() {
    const row = this.row;
    this.row = [];
    this.process({ done: false, value: row });
  }

  private onEnd() {
    this.done = true;
    this.process({ done: true, value: undefined });
  }

  private process(result: IteratorResult<string[], void> | Error) {
    const cb = result instanceof Error
      ? this.nextPromiseReject
      : this.nextPromiseResolve;

    if (cb) {
      this.nextPromise = undefined;
      this.nextPromiseResolve = undefined;
      this.nextPromiseReject = undefined;
      cb(result as any);
    } else {
      this.buffer.push(result);
    }

    this.reader.pause();
  }

  next(): Promise<IteratorResult<string[], void>> {
    if (this.done && this.buffer.length === 0) {
      return Promise.resolve({ done: true, value: undefined });
    }

    let promise = this.nextPromise;
    if (!promise) {
      if (this.buffer.length > 0) {
        const res = this.buffer.shift()!;
        if (res instanceof Error) {
          return Promise.reject(res);
        } else {
          return Promise.resolve(res);
        }
      }

      promise = new Promise((resolve, reject) => {
        this.nextPromiseResolve = resolve;
        this.nextPromiseReject = reject;
        this.reader.read();
      });
    }

    // not resolved yet
    if (this.nextPromiseResolve) {
      this.nextPromise = promise;
    }

    return promise;
  }

  [Symbol.asyncIterator]() {
    return this;
  }
}

/** Read CSV as stream of arrays of cells:
 *
 *       for await (const row of readCSVRows(f)) {
 *         console.log(`row: ${row.join(' ')}`)
 *       }
 */
export function readCSVRows(
  reader: Deno.Reader,
  options?: Partial<CommonCSVReaderOptions>,
): AsyncIterable<string[]> {
  return new CSVRowReader(reader, options);
}

class RowIterator implements AsyncIterableIterator<string> {
  private onRequested: () => Promise<IteratorResult<string | symbol>>;
  private done: boolean;

  constructor(onRequested: () => Promise<IteratorResult<string | symbol>>) {
    this.onRequested = onRequested;
    this.done = false;
  }

  async readTillEnd() {
    if (this.done) {
      return;
    }

    for await (const _ of this) {
      // just read all cells
    }
  }

  async next(): Promise<IteratorResult<string, any>> {
    if (this.done) {
      return { done: true, value: null };
    }

    const { done, value } = await this.onRequested();

    if (done || value === newLine) {
      this.done = true;
      return { done: true, value: null };
    } else {
      return { done: false, value: value as string };
    }
  }

  [Symbol.asyncIterator]() {
    return this;
  }
}

class CSVRowIteratorReader implements AsyncIterableIterator<RowIterator> {
  private reader: CSVReader;
  private done: boolean;
  private rowIterator: RowIterator | undefined;
  private buffer: Array<IteratorResult<string | symbol, void> | Error>;
  private nextPromise?: Promise<IteratorResult<string | symbol, void>>;
  private nextPromiseResolve?: (
    res: IteratorResult<string | symbol, void>,
  ) => void;
  private nextPromiseReject?: (err: Error) => void;

  constructor(reader: Deno.Reader, options?: Partial<CommonCSVReaderOptions>) {
    this.done = false;
    this.buffer = [];
    this.reader = new CSVReader(reader, {
      ...options,
      onCell: (value) => this.onCell(value),
      onRowEnd: () => this.onRowEnd(),
      onEnd: () => this.onEnd(),
      onError: (err) => this.onError(err),
    });
  }

  private onCell(value: string) {
    this.process({ done: false, value });
  }

  private onRowEnd() {
    this.process({ done: false, value: newLine });
  }

  private onEnd() {
    this.done = true;
    this.process({ done: true, value: undefined });
  }

  private onError(err: Error) {
    this.process(err);
  }

  private process(result: IteratorResult<string | symbol, void> | Error) {
    const cb = result instanceof Error
      ? this.nextPromiseReject
      : this.nextPromiseResolve;

    if (cb) {
      this.nextPromise = undefined;
      this.nextPromiseResolve = undefined;
      this.nextPromiseReject = undefined;
      cb(result as any);
    } else {
      this.buffer.push(result);
    }

    this.reader.pause();
  }

  onRequested(): Promise<IteratorResult<string | symbol>> {
    let promise = this.nextPromise;
    if (!promise) {
      if (this.buffer.length > 0) {
        const res = this.buffer.shift()!;
        if (res instanceof Error) {
          return Promise.reject(res);
        } else {
          return Promise.resolve(res);
        }
      }

      promise = new Promise((resolve, reject) => {
        this.nextPromiseResolve = resolve;
        this.nextPromiseReject = reject;
        this.reader.read();
      });
    }

    // not resolved yet
    if (this.nextPromiseResolve) {
      this.nextPromise = promise;
    }

    return promise;
  }

  async next(): Promise<IteratorResult<RowIterator, void>> {
    if (this.rowIterator) {
      await this.rowIterator.readTillEnd();
      this.rowIterator = undefined;
    }

    if (this.done) {
      return { done: true, value: undefined };
    }

    this.rowIterator = new RowIterator(() => this.onRequested());

    return { done: false, value: this.rowIterator };
  }

  [Symbol.asyncIterator]() {
    return this;
  }
}

/** Read CSV as stream of steams of cells:
 *
 *       for await (const row of readCSV(f)) {
 *         console.log('row:')
 *         for await (const cell of row) {
 *           console.log(`  cell: ${cell}`);
 *         }
 *       }
 */
export function readCSV(
  reader: Deno.Reader,
  options?: Partial<CommonCSVReaderOptions>,
): AsyncIterable<AsyncIterable<string>> {
  return new CSVRowIteratorReader(reader, options);
}

/** Read CSV as stream of objects:
 *
 *       for await (const obj of readCSVObjects(f)) {
 *         console.log(obj);
 *       }
 */
export async function* readCSVObjects(
  reader: Deno.Reader,
  options?: Partial<CommonCSVReaderOptions>,
): AsyncIterable<{ [key: string]: string }> {
  let header: string[] | undefined;

  for await (const row of new CSVRowReader(reader, options)) {
    if (!header) {
      header = row;
      continue;
    }

    const obj: { [key: string]: string } = {};
    for (let i = 0; i < header.length; i++) {
      obj[header[i]] = row[i];
    }

    yield obj;
  }
}

function findReadTillIndex(
  a: Uint8Array,
  limit: number,
  pat: Uint8Array,
): number {
  const s = pat[0];

  for (let i = 0; i < a.length; i++) {
    if (i >= limit) {
      return limit;
    }

    if (a[i] === s) {
      let matched = 1;
      let j = i;
      while (matched < pat.length) {
        j++;
        if (a[j] !== pat[j - i]) {
          break;
        }
        matched++;
      }
      if (matched === pat.length) {
        return i;
      }
    }
  }

  return limit;
}

function findReadTillIndex3(
  a: Uint8Array,
  limit: number,
  pat1: Uint8Array,
  pat2: Uint8Array,
  pat3: Uint8Array,
): number {
  const s1 = pat1[0];
  const s2 = pat2[0];
  const s3 = pat3[0];

  for (let i = 0; i < a.length; i++) {
    if (i >= limit) {
      return limit;
    }

    if (a[i] === s1) {
      let matched = 1;
      let j = i;
      while (matched < pat1.length) {
        j++;
        if (a[j] !== pat1[j - i]) {
          break;
        }
        matched++;
      }
      if (matched === pat1.length) {
        return i;
      }
    }

    if (a[i] === s2) {
      let matched = 1;
      let j = i;
      while (matched < pat2.length) {
        j++;
        if (a[j] !== pat2[j - i]) {
          break;
        }
        matched++;
      }
      if (matched === pat2.length) {
        return i;
      }
    }

    if (a[i] === s3) {
      let matched = 1;
      let j = i;
      while (matched < pat3.length) {
        j++;
        if (a[j] !== pat3[j - i]) {
          break;
        }
        matched++;
      }
      if (matched === pat3.length) {
        return i;
      }
    }
  }

  return limit;
}
