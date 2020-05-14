import { hasPrefix, repeat, concat } from "https://deno.land/std/bytes/mod.ts";
import { getLogger } from "https://deno.land/std/log/mod.ts";

export interface CSVReaderOptions {
  columnSeparator: Uint8Array;
  lineSeparator: Uint8Array;
  quote: Uint8Array;
}

const defaultCSVReaderOptions = {
  columnSeparator: new Uint8Array([44]), // ,
  lineSeparator: new Uint8Array([10]), // \n
  quote: new Uint8Array([34]), // "
};

const ITER_READ_BUFFER_SIZE = 1024;
const COLUMN_BUFFER_MIN_STEP_SIZE = 1024;
const INPUT_BUFFER_INDEX_LIMIT = 1024;

function debug(msg: string) {
  getLogger("csv").debug(msg);
}

function hasPrefixFrom(a: Uint8Array, prefix: Uint8Array, offset: number) {
  return hasPrefix(offset > 0 ? a.subarray(offset) : a, prefix);
}

export async function* readCSV(
  reader: Deno.Reader,
  options: CSVReaderOptions = defaultCSVReaderOptions,
): AsyncIterableIterator<string[]> {
  const { columnSeparator, lineSeparator, quote } = options;
  const doubleQuote = repeat(quote, 2);

  const decoder = new TextDecoder();

  const minPossibleBufferReserve = Math.max(
    columnSeparator.length,
    lineSeparator.length,
    doubleQuote.length,
    1,
  );
  const columnBufferStepSize = Math.max(
    COLUMN_BUFFER_MIN_STEP_SIZE,
    minPossibleBufferReserve,
  );

  const readerIterator = Deno.iter(reader, {
    bufSize: ITER_READ_BUFFER_SIZE,
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

  while (true) {
    const inputBufferUnprocessed = inputBuffer.length - inputBufferIndex;

    // lacks of data
    if (!readerEmpty && inputBufferUnprocessed < minPossibleBufferReserve) {
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
    if (inputBufferIndex >= INPUT_BUFFER_INDEX_LIMIT) {
      debug("slice buffer");
      inputBuffer = inputBuffer.slice(inputBufferIndex);
      inputBufferIndex = 0;
      continue;
    }

    // column buffer is almost full
    if (columnBuffer.length - columnBufferIndex < minPossibleBufferReserve) {
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

    if (!inColumn && inputBufferUnprocessed === 0) {
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
      continue;
    }

    if (
      inColumn &&
      !inQuote &&
      (inputBufferUnprocessed === 0 ||
        hasNext(lineSeparator) ||
        hasNext(columnSeparator))
    ) {
      debug("end unquoted column");
      inColumn = false;
      continue;
    }

    if (inColumn && inputBufferUnprocessed > 0) {
      debug("read char");
      columnBuffer[columnBufferIndex++] = inputBuffer[inputBufferIndex++];
      continue;
    }

    throw new Error("unexpected");
  }
}

export async function* readCSVObjects(
  reader: Deno.Reader,
  options: CSVReaderOptions = defaultCSVReaderOptions,
): AsyncIterableIterator<{ [key: string]: string }> {
  let header: string[] | undefined;

  for await (const row of readCSV(reader, options)) {
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
