import { concat, getLogger, iterateReader, Logger, repeat } from "./deps.ts";
import { getUint8Array, hasPrefixFrom } from "./utils.ts";

/** Common options for CSV reader module */
export interface CommonCSVReaderOptions {
  columnSeparator: string | Uint8Array;
  lineSeparator: string | Uint8Array;
  quote: string | Uint8Array;
  encoding?: string;
  fromLine?: number;
  toLine?: number;
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
  _columnBufferReserve: number;
  _stats: {
    reads: number;
    inputBufferShrinks: number;
    columnBufferExpands: number;
  };
}

// deno-lint-ignore no-explicit-any
function noop(_?: any): any {}

const utfBom = new Uint8Array([0xef, 0xbb, 0xbf]);

const defaultCSVReaderOptions: HiddenCSVReaderOptions = {
  columnSeparator: ",",
  lineSeparator: "\n",
  quote: '"',
  onCell: noop,
  onRowEnd: noop,
  onEnd: noop,
  onError: noop,
  _readerIteratorBufferSize: 1024,
  _columnBufferMinStepSize: 1024,
  _inputBufferIndexLimit: 1024,
  _columnBufferReserve: 64,
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
  private inputBufferIndexLimit: number;
  private stats: {
    reads: number;
    inputBufferShrinks: number;
    columnBufferExpands: number;
  };
  private columnSeparator: Uint8Array;
  private lineSeparator: Uint8Array;
  private quote: Uint8Array;
  private doubleQuote: Uint8Array;
  private minPossibleBufferReserve: number;
  private columnBufferReserve: number;
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
  private currentPos: number;
  private linesProcessed: number;
  private lastLineStartPos: number;
  private fromLine: number;
  private toLine: number;

  constructor(reader: Deno.Reader, options?: Partial<CSVReaderOptions>) {
    this.decoder = new TextDecoder(options?.encoding);
    const mergedOptions: HiddenCSVReaderOptions = {
      ...defaultCSVReaderOptions,
      ...options,
    };
    this.fromLine = mergedOptions.fromLine || 0;
    this.toLine = mergedOptions.toLine || Number.MAX_VALUE;
    this.onCell = mergedOptions.onCell || noop;
    this.onRowEnd = mergedOptions.onRowEnd || noop;
    this.onEnd = mergedOptions.onEnd || noop;
    this.onError = mergedOptions.onError || noop;
    this.inputBufferIndexLimit = mergedOptions._inputBufferIndexLimit;
    this.stats = mergedOptions._stats;
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
      mergedOptions._columnBufferMinStepSize,
      this.minPossibleBufferReserve,
    );
    this.columnBufferReserve = Math.max(
      mergedOptions._columnBufferReserve,
      this.minPossibleBufferReserve,
    );

    this.readerIterator = iterateReader(reader, {
      bufSize: mergedOptions._readerIteratorBufferSize,
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

    this.currentPos = 0;
    this.linesProcessed = 0;
    this.lastLineStartPos = 0;

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

  private skip(length: number) {
    this.debug(`skip: ${length}`);
    this.inputBufferIndex += length;
    this.inputBufferUnprocessed -= length;
    this.currentPos += length;
  }

  private shrinkInputBuffer() {
    this.stats.inputBufferShrinks++;
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
    this.currentPos += n;
  }

  private async readMoreData() {
    this.stats.reads++;
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
    this.stats.columnBufferExpands++;
    const newColumn = new Uint8Array(
      this.columnBuffer.length + this.columnBufferStepSize,
    );
    this.debug(
      `expand column buffer from ${this.columnBuffer.length} to ${newColumn.length}`,
    );
    newColumn.set(this.columnBuffer);
    this.columnBuffer = newColumn;
  }

  private countLine() {
    this.countLines(1, this.currentPos);
  }

  private countLines(newLines: number, lastLineStartPos: number) {
    this.debug(
      `count lines: newLines=${newLines} lastLineStartPos=${lastLineStartPos}`,
    );
    this.linesProcessed += newLines;
    this.lastLineStartPos = lastLineStartPos;
  }

  private getCurrentPos() {
    const line = this.linesProcessed + 1;
    const ch = this.currentPos - this.lastLineStartPos + 1;

    return `line ${line}, character ${ch}`;
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
      if (this.inputBufferIndex >= this.inputBufferIndexLimit) {
        this.shrinkInputBuffer();
        continue;
      }

      // column buffer is almost full
      if (
        this.columnBuffer.length - this.columnBufferIndex <
          this.columnBufferReserve
      ) {
        this.expandColumnBuffer();
        continue;
      }

      // skip line if it didn't reach fromLine
      if (!this.inColumn && this.linesProcessed < this.fromLine) {
        const slice = this.inputBuffer.subarray(this.inputBufferIndex);
        const index = findReadTillLineSeparatorIndex(slice, this.lineSeparator);
        if (index === null) {
          this.skip(slice.length);
          continue;
        }
        this.skip(index + this.lineSeparator.length);
        this.countLine();
        this.emptyLine = true;
        continue;
      }

      // linesProcessed start at 1 and toLine at 0
      // stop reading if toLine is reached
      if (!this.inColumn && this.linesProcessed >= this.toLine) {
        this.debug("eof");
        this.onEnd();
        return;
      }

      // skip UTF BOM
      if (!this.inColumn && this.currentPos === 0 && this.hasNext(utfBom)) {
        this.skip(utfBom.length);
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
        this.skip(this.lineSeparator.length);
        this.countLine();
        this.emptyLine = true;
        continue;
      }

      if (!this.inColumn && this.hasNext(this.columnSeparator)) {
        this.debug("columnSeparator");
        this.emptyLine = false;
        this.processColumn();
        this.skip(this.columnSeparator.length);
        continue;
      }

      if (!this.inColumn) {
        this.inColumn = true;
        this.emptyLine = false;
        if (this.hasNext(this.quote)) {
          this.debug("start quoted column");
          this.inQuote = true;
          this.skip(this.quote.length);
        } else {
          this.debug("start unquoted column");
        }
        continue;
      }

      if (this.inColumn && this.inQuote && this.hasNext(this.doubleQuote)) {
        this.debug("double quote");
        this.columnBuffer.set(this.quote, this.columnBufferIndex);
        this.columnBufferIndex += this.quote.length;
        this.skip(this.doubleQuote.length);
        continue;
      }

      if (this.inColumn && this.inQuote && this.hasNext(this.quote)) {
        this.debug("end quoted column");
        this.inQuote = false;
        this.inColumn = false;
        this.skip(this.quote.length);
        if (
          this.inputBufferUnprocessed > 0 &&
          !this.hasNext(this.lineSeparator) &&
          !this.hasNext(this.columnSeparator)
        ) {
          const charCode = this.inputBuffer[this.inputBufferIndex];
          const char = charCode === 13 ? "\\r" : String.fromCharCode(charCode);
          let msg =
            `Expected EOF, COLUMN_SEPARATOR, LINE_SEPARATOR; received ${char} (${this.getCurrentPos()})`;
          if (charCode === 13) {
            msg +=
              '\nPerhaps you need to add the setting lineSeparator: "\\r\\n"\nhttps://git.io/JDTDS';
          }
          this.onError(new Error(msg));
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
        const limit = Math.min(
          slice.length - this.minPossibleBufferReserve,
          this.columnBuffer.length - this.columnBufferIndex,
        );

        let readTillIndex = 1;
        let newLines = 0;
        let lastLineStartPos = -1;
        if (limit > 1) {
          if (this.inQuote) {
            const { till, lineSeparatorsFound, lastLineSeparatorEndIndex } =
              findReadTillIndexQuoted(
                slice,
                limit,
                this.quote,
                this.lineSeparator,
              );

            readTillIndex = till;
            newLines = lineSeparatorsFound;
            lastLineStartPos = this.currentPos + lastLineSeparatorEndIndex;
          } else {
            const { till, type } = findReadTillIndex(
              slice,
              limit,
              this.lineSeparator,
              this.columnSeparator,
              this.quote,
            );

            if (till === 0 && type === FindReadTillIndexType.QUOTE) {
              this.onError(
                new Error(
                  `Unexpected quote in unquoted field (${this.getCurrentPos()})`,
                ),
              );
              return;
            }

            readTillIndex = till;
          }
        }

        if (readTillIndex > 0) {
          this.debug(`read char: ${readTillIndex}`);
          this.readChars(readTillIndex);
        }
        if (newLines > 0) {
          this.countLines(newLines, lastLineStartPos);
        }
        continue;
      }

      if (this.inQuote && this.inputBufferUnprocessed === 0) {
        this.onError(
          new Error(`Expected quote, received EOF (${this.getCurrentPos()})`),
        );
        return;
      }

      this.onError(new Error(`unexpected (${this.getCurrentPos()})`));
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
      // deno-lint-ignore no-explicit-any
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
      // deno-lint-ignore no-explicit-any
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
  private buffer: Array<IteratorResult<string | symbol>>;
  private done: boolean;

  constructor(onRequested: () => Promise<IteratorResult<string | symbol>>) {
    this.onRequested = onRequested;
    this.buffer = [];
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

  async lookForward(): Promise<IteratorResult<string | symbol>> {
    const res = await this.onRequested();
    this.buffer.push(res);
    return res;
  }

  async next(): Promise<IteratorResult<string>> {
    if (this.done) {
      return { done: true, value: null };
    }

    const { done, value } = this.buffer.length > 0
      ? this.buffer.shift() as IteratorResult<string | symbol>
      : await this.onRequested();

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
      // deno-lint-ignore no-explicit-any
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
    const nextCell = await this.rowIterator.lookForward();
    if (this.done && nextCell.done) {
      return { done: true, value: undefined };
    }

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

function findReadTillIndexQuoted(
  a: Uint8Array,
  limit: number,
  quote: Uint8Array,
  lineSeparator: Uint8Array,
): {
  till: number;
  lineSeparatorsFound: number;
  lastLineSeparatorEndIndex: number;
} {
  const s1 = quote[0];
  const s2 = lineSeparator[0];
  let result = limit;
  let lineSeparatorsFound = 0;
  let lastLineSeparatorEndIndex = -1;

  for (let i = 0; i < a.length; i++) {
    if (i >= limit) {
      result = limit;
      break;
    }

    if (a[i] === s1) {
      let matched = 1;
      let j = i;
      while (matched < quote.length) {
        j++;
        if (a[j] !== quote[j - i]) {
          break;
        }
        matched++;
      }
      if (matched === quote.length) {
        result = i;
        break;
      }
    }

    if (a[i] === s2) {
      let matched = 1;
      let j = i;
      while (matched < lineSeparator.length) {
        j++;
        if (a[j] !== lineSeparator[j - i]) {
          break;
        }
        matched++;
      }
      if (matched === lineSeparator.length) {
        lineSeparatorsFound++;
        lastLineSeparatorEndIndex = i + lineSeparator.length;
        i += lineSeparator.length - 1;
      }
    }
  }

  return { till: result, lineSeparatorsFound, lastLineSeparatorEndIndex };
}

enum FindReadTillIndexType {
  LIMIT = 0,
  LINE_SEPARATOR = 1,
  COLUMN_SEPARATOR = 2,
  QUOTE = 3,
}

function findReadTillIndex(
  a: Uint8Array,
  limit: number,
  lineSeparator: Uint8Array,
  columnSeparator: Uint8Array,
  quote: Uint8Array,
): { till: number; type: FindReadTillIndexType } {
  const s1 = lineSeparator[0];
  const s2 = columnSeparator[0];
  const s3 = quote[0];

  for (let i = 0; i < a.length; i++) {
    if (i >= limit) {
      return { till: limit, type: FindReadTillIndexType.LIMIT };
    }

    if (a[i] === s1) {
      let matched = 1;
      let j = i;
      while (matched < lineSeparator.length) {
        j++;
        if (a[j] !== lineSeparator[j - i]) {
          break;
        }
        matched++;
      }
      if (matched === lineSeparator.length) {
        return { till: i, type: FindReadTillIndexType.LINE_SEPARATOR };
      }
    }

    if (a[i] === s2) {
      let matched = 1;
      let j = i;
      while (matched < columnSeparator.length) {
        j++;
        if (a[j] !== columnSeparator[j - i]) {
          break;
        }
        matched++;
      }
      if (matched === columnSeparator.length) {
        return { till: i, type: FindReadTillIndexType.COLUMN_SEPARATOR };
      }
    }

    if (a[i] === s3) {
      let matched = 1;
      let j = i;
      while (matched < quote.length) {
        j++;
        if (a[j] !== quote[j - i]) {
          break;
        }
        matched++;
      }
      if (matched === quote.length) {
        return { till: i, type: FindReadTillIndexType.QUOTE };
      }
    }
  }

  return { till: limit, type: FindReadTillIndexType.LIMIT };
}

function findReadTillLineSeparatorIndex(
  a: Uint8Array,
  lineSeparator: Uint8Array,
): number | null {
  const s1 = lineSeparator[0];

  for (let i = 0; i < a.length; i++) {
    if (a[i] === s1) {
      let matched = 1;
      let j = i;
      while (matched < lineSeparator.length) {
        j++;
        if (a[j] !== lineSeparator[j - i]) {
          break;
        }
        matched++;
      }
      if (matched === lineSeparator.length) {
        return i;
      }
    }
  }

  return null;
}
