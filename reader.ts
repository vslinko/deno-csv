import { repeat, concat } from "./deps.ts";
import { hasPrefixFrom, debug, getUint8Array } from "./utils.ts";

export interface CSVReaderOptions {
  columnSeparator: string | Uint8Array;
  lineSeparator: string | Uint8Array;
  quote: string | Uint8Array;
  _readerIteratorBufferSize: number;
  _columnBufferMinStepSize: number;
  _inputBufferIndexLimit: number;
  _stats: {
    reads: number;
    inputBufferShrinks: number;
    columnBufferExpands: number;
  };
}

const defaultCSVReaderOptions = {
  columnSeparator: ",",
  lineSeparator: "\n",
  quote: '"',
  _readerIteratorBufferSize: 1024,
  _columnBufferMinStepSize: 1024,
  _inputBufferIndexLimit: 1024,
  _stats: {
    reads: 0,
    inputBufferShrinks: 0,
    columnBufferExpands: 0,
  },
};

class Row implements AsyncIterableIterator<string> {
  private onRequested: () => Promise<IteratorResult<string | symbol>>;

  constructor(onRequested: () => Promise<IteratorResult<string | symbol>>) {
    this.onRequested = onRequested;
  }

  async next(): Promise<IteratorResult<string, any>> {
    const { done, value } = await this.onRequested();

    if (done) {
      return { done: true, value: null };
    } else if (value === newLine) {
      return { done: true, value: null };
    } else {
      return { done: false, value: value as string };
    }
  }

  [Symbol.asyncIterator]() {
    return this;
  }
}

export async function* readCSV(
  reader: Deno.Reader,
  options?: Partial<CSVReaderOptions>,
): AsyncIterableIterator<AsyncIterableIterator<string>> {
  const iter = readCSVStream(reader, options);
  let ended = false;

  const onRequested = async () => {
    const { done, value } = await iter.next();
    if (done) {
      ended = true;
    }
    return { done, value };
  };

  while (!ended) {
    yield new Row(onRequested);
  }
}

export async function* readCSVRows(
  reader: Deno.Reader,
  options?: Partial<CSVReaderOptions>,
): AsyncIterableIterator<string[]> {
  for await (const rowIter of readCSV(reader, options)) {
    const row: string[] = [];
    for await (const cell of rowIter) {
      row.push(cell);
    }
    yield row;
  }
}

export const newLine = Symbol.for("newLine");

export async function* readCSVStream(
  reader: Deno.Reader,
  options?: Partial<CSVReaderOptions>,
): AsyncIterableIterator<string | symbol> {
  const mergedOptions = {
    ...defaultCSVReaderOptions,
    ...options,
  };
  const {
    _readerIteratorBufferSize,
    _columnBufferMinStepSize,
    _inputBufferIndexLimit,
    _stats,
  } = mergedOptions;
  const quote = getUint8Array(mergedOptions.quote);
  const columnSeparator = getUint8Array(mergedOptions.columnSeparator);
  const lineSeparator = getUint8Array(mergedOptions.lineSeparator);
  const doubleQuote = repeat(quote, 2);

  const decoder = new TextDecoder();

  const minPossibleBufferReserve = Math.max(
    columnSeparator.length,
    lineSeparator.length,
    doubleQuote.length,
    1,
  );
  const columnBufferStepSize = Math.max(
    _columnBufferMinStepSize,
    minPossibleBufferReserve,
  );

  const readerIterator = Deno.iter(reader, {
    bufSize: _readerIteratorBufferSize,
  });

  let inputBuffer = new Uint8Array();
  let inputBufferIndex = 0;

  let columnBuffer = new Uint8Array(columnBufferStepSize);
  let columnBufferIndex = 0;

  let readerEmpty = false;
  let emptyLine = true;
  let inQuote = false;
  let inColumn = false;

  const getAndResetColumn = () => {
    const result = decoder.decode(columnBuffer.subarray(0, columnBufferIndex));
    columnBuffer = new Uint8Array(columnBufferStepSize);
    columnBufferIndex = 0;
    return result;
  };
  const hasNext = (chars: Uint8Array) => {
    return hasPrefixFrom(inputBuffer, chars, inputBufferIndex);
  };
  const skip = (chars: Uint8Array) => {
    inputBufferIndex += chars.length;
  };
  const inputBufferUnprocessed = () => inputBuffer.length - inputBufferIndex;

  while (true) {
    // lacks of data
    if (!readerEmpty && inputBufferUnprocessed() < minPossibleBufferReserve) {
      _stats.reads++;
      debug("read more data");
      const { done, value } = await readerIterator.next();
      if (done) {
        readerEmpty = true;
      } else {
        inputBuffer = concat(inputBuffer, value);
      }
      continue;
    }

    // buffer could be emptied
    if (inputBufferIndex >= _inputBufferIndexLimit) {
      _stats.inputBufferShrinks++;
      debug("shrink input buffer");
      inputBuffer = inputBuffer.slice(inputBufferIndex);
      inputBufferIndex = 0;
      continue;
    }

    // column buffer is almost full
    if (columnBuffer.length - columnBufferIndex < minPossibleBufferReserve) {
      _stats.columnBufferExpands++;
      const newColumn = new Uint8Array(
        columnBuffer.length + columnBufferStepSize,
      );
      debug(
        `expand column buffer from ${columnBuffer.length} to ${newColumn.length}`,
      );
      newColumn.set(columnBuffer);
      columnBuffer = newColumn;
      continue;
    }

    if (!inColumn && inputBufferUnprocessed() === 0) {
      debug("eof");
      if (!emptyLine) {
        yield getAndResetColumn();
      }
      return;
    }

    if (!inColumn && hasNext(lineSeparator)) {
      debug("lineSeparator");
      if (!emptyLine) {
        yield getAndResetColumn();
        yield newLine;
      }
      skip(lineSeparator);
      emptyLine = true;
      continue;
    }

    if (!inColumn && hasNext(columnSeparator)) {
      debug("columnSeparator");
      yield getAndResetColumn();
      skip(columnSeparator);
      continue;
    }

    if (!inColumn) {
      inColumn = true;
      emptyLine = false;
      if (hasNext(quote)) {
        debug("start quoted column");
        inQuote = true;
        skip(quote);
      } else {
        debug("start unquoted column");
      }
      continue;
    }

    if (inColumn && inQuote && hasNext(doubleQuote)) {
      debug("double quote");
      columnBuffer.set(quote, columnBufferIndex);
      columnBufferIndex += quote.length;
      skip(doubleQuote);
      continue;
    }

    if (inColumn && inQuote && hasNext(quote)) {
      debug("end quoted column");
      inQuote = false;
      inColumn = false;
      skip(quote);
      if (
        inputBufferUnprocessed() > 0 &&
        !hasNext(lineSeparator) &&
        !hasNext(columnSeparator)
      ) {
        const char = String.fromCharCode(inputBuffer[inputBufferIndex]);
        throw new Error(
          `Expected EOF, COLUMN_SEPARATOR, LINE_SEPARATOR; received ${char}`,
        );
      }
      continue;
    }

    if (
      inColumn &&
      !inQuote &&
      (inputBufferUnprocessed() === 0 ||
        hasNext(lineSeparator) ||
        hasNext(columnSeparator))
    ) {
      debug("end unquoted column");
      inColumn = false;
      continue;
    }

    if (inColumn && inputBufferUnprocessed() > 0) {
      debug("read char");
      columnBuffer[columnBufferIndex++] = inputBuffer[inputBufferIndex++];
      continue;
    }

    if (inQuote && inputBufferUnprocessed() === 0) {
      throw new Error("Expected quote, received EOF");
    }

    throw new Error("unexpected");
  }
}

export async function* readCSVObjects(
  reader: Deno.Reader,
  options?: Partial<CSVReaderOptions>,
): AsyncIterableIterator<{ [key: string]: string }> {
  let header: string[] | undefined;

  for await (const row of readCSVRows(reader, options)) {
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
