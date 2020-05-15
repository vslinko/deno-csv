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

export async function* readCSV(
  reader: Deno.Reader,
  options?: Partial<CSVReaderOptions>,
): AsyncIterableIterator<AsyncIterableIterator<string>> {
  const rowIter = async function* (
    row: string[],
  ): AsyncIterableIterator<string> {
    for (const cell of row) {
      yield cell;
    }
  };

  for await (const row of _readCSV(reader, options)) {
    yield rowIter(row);
  }
}

export async function* readCSVRows(
  reader: Deno.Reader,
  options?: Partial<CSVReaderOptions>,
): AsyncIterableIterator<string[]> {
  for await (const row of _readCSV(reader, options)) {
    yield row;
  }
}

async function* _readCSV(
  reader: Deno.Reader,
  options?: Partial<CSVReaderOptions>,
): AsyncIterableIterator<string[]> {
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

  let row: string[] = [];

  let columnBuffer = new Uint8Array(columnBufferStepSize);
  let columnBufferIndex = 0;

  let readerEmpty = false;
  let emptyLine = true;
  let inQuote = false;
  let inColumn = false;

  const appendColumn = () => {
    row.push(decoder.decode(columnBuffer.subarray(0, columnBufferIndex)));
    columnBuffer = new Uint8Array(columnBufferStepSize);
    columnBufferIndex = 0;
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
        appendColumn();
        yield row;
      }
      return;
    }

    if (!inColumn && hasNext(lineSeparator)) {
      debug("lineSeparator");
      if (!emptyLine) {
        appendColumn();
        yield row;
        row = [];
      }
      skip(lineSeparator);
      emptyLine = true;
      continue;
    }

    if (!inColumn && hasNext(columnSeparator)) {
      debug("columnSeparator");
      appendColumn();
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
