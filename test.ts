import { wasmCode } from "./test_wasm.ts";

function kbToPages(kb: number) {
  return kb / 65536;
}

const wasmModule = new WebAssembly.Module(wasmCode);

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

enum State {
  BEGIN = 0,
  NEED_MORE_DATA = 1,
  EOF = 2,
  ERROR = 3,
  CELL = 14,
  CELL_AND_NEWLINE = 15,
  CELL_AND_EOF = 16,
}

export interface CSVReaderOptions {
  columnSeparator: string;
  lineSeparator: string;
  quote: string;
  memoryLimitKb: number;
  onCell(cell: string): void;
  onRowEnd(): void;
  onEnd(): void;
  onError(err: Error): void;
}

function noop(a?: any) {}

const csvReaderDefaultOptions: CSVReaderOptions = {
  columnSeparator: ",",
  lineSeparator: "\n",
  quote: '"',
  memoryLimitKb: 10 * 1024 * 1024,
  onCell: noop,
  onRowEnd: noop,
  onEnd: noop,
  onError: noop,
};

export class CSVReader {
  private inputReader: Deno.Reader;
  private memory: WebAssembly.Memory;
  private instance: WebAssembly.Instance;
  private onCell: (cell: string) => void;
  private onRowEnd: () => void;
  private onEnd: () => void;
  private onError: (err: Error) => void;

  constructor(inputReader: Deno.Reader, options?: Partial<CSVReaderOptions>) {
    const resultOptions: CSVReaderOptions = {
      ...csvReaderDefaultOptions,
      ...options,
    };

    this.onCell = resultOptions.onCell;
    this.onRowEnd = resultOptions.onRowEnd;
    this.onEnd = resultOptions.onEnd;
    this.onError = resultOptions.onError;

    this.inputReader = inputReader;

    this.memory = new WebAssembly.Memory({
      initial: 3, // 1st = preferences, 2nd = input buffer, 3rd+ = cell content
      maximum: kbToPages(resultOptions.memoryLimitKb),
    });

    this.instance = new WebAssembly.Instance(wasmModule, {
      js: {
        mem: this.memory,
        log: (n: number) => console.log(n),
      },
    });
    this.setColumnSeparator(resultOptions.columnSeparator);
    this.setLineSeparator(resultOptions.lineSeparator);
    this.setQuote(resultOptions.quote);
    this.instance.exports.setup();
  }

  public setColumnSeparator(value: string) {
    this.setStringValue("col_sep", value);
  }

  public getColumnSeparator() {
    return this.getStringValue("col_sep");
  }

  public setLineSeparator(value: string) {
    this.setStringValue("line_sep", value);
  }

  public getLineSeparator() {
    return this.getStringValue("line_sep");
  }

  public setQuote(value: string) {
    this.setStringValue("qoute", value);
  }

  public getQuote() {
    return this.getStringValue("qoute");
  }

  public async read() {
    await this.readMoreData();

    while (true) {
      const state = this.tick();

      if (state === State.ERROR) {
        // TODO
        this.onError(new Error(`Some error`));
        break;
      } else if (state === State.NEED_MORE_DATA) {
        const read = await this.readMoreData();
        // console.log('NEED_MORE_DATA', read)
      } else if (state === State.CELL_AND_NEWLINE) {
        this.onCell(this.readCell());
        this.onRowEnd();
      } else if (state === State.CELL_AND_EOF) {
        this.onCell(this.readCell());
        this.onRowEnd();
        this.onEnd();
        break;
      } else if (state === State.CELL) {
        this.onCell(this.readCell());
      } else {
        this.onError(new Error(`unexpected state ${State[state]}`));
        break;
      }
    }
  }

  private readCell() {
    const cellWriteIdx = this.instance.exports.getCellWriteIdx();
    const cell = new Uint8Array(this.memory.buffer, this.instance.exports.cell_idx.value, cellWriteIdx)
    return textDecoder.decode(cell);
  }

  private async readMoreData() {
    const buf = new Uint8Array(
      this.memory.buffer,
      this.instance.exports.input_idx.value,
      this.instance.exports.input_size.value,
    );

    const prefRead = new Uint32Array(this.memory.buffer, this.instance.exports.input_len_idx, 1)[0];
    const inputReadIdx = this.instance.exports.getInputReadIndex();
    const left = prefRead - inputReadIdx; 
    if (left > 0) {
      buf.set(buf.subarray(inputReadIdx, inputReadIdx + left), 0);
    }

    const nextRead = await this.inputReader.read(buf.subarray(left));

    if (nextRead === null) {
      this.instance.exports.setEof();
      return null;
    }

    const len = left + nextRead;

    this.setUint32(this.instance.exports.input_len_idx.value, len);
    this.instance.exports.resetInputIndex();

    return nextRead;
  }

  private tick() {
    const res: State = this.instance.exports.read();
    return res;
  }

  private setStringValue(prefix: string, value: string) {
    this.setValue(prefix, textEncoder.encode(value));
  }

  private setValue(prefix: string, value: Uint8Array) {
    const size = this.instance.exports[prefix + "_size"];
    const lenIdx = this.instance.exports[prefix + "_len_idx"];
    const idx = this.instance.exports[prefix + "_idx"];
    if (value.length > size) {
      throw new Error(`${prefix} length shouldn't be more than ${size} bytes`);
    }
    this.setUint32(lenIdx, value.length);
    this.setUint8Array(idx, value);
  }

  private getStringValue(prefix: string) {
    return textDecoder.decode(this.getValue(prefix));
  }

  private getValue(prefix: string) {
    const lenIdx = this.instance.exports[prefix + "_len_idx"];
    const idx = this.instance.exports[prefix + "_idx"];
    const len = new Uint32Array(this.memory.buffer, lenIdx, 1)[0];
    const value = new Uint8Array(this.memory.buffer, idx, len);
    return value;
  }

  private setUint32(offset: number, value: number) {
    new Uint32Array(this.memory.buffer, offset, 1)[0] = value;
  }

  private setUint8Array(offset: number, value: Uint8Array) {
    new Uint8Array(this.memory.buffer).set(value, offset);
  }
}

if (import.meta.main) {
  class MyReader implements Deno.Reader {
    private buf: Uint8Array;
    private index: number;

    constructor(content: string) {
      this.buf = new TextEncoder().encode(content);
      this.index = 0;
    }

    public async read(p: Uint8Array): Promise<number | null> {
      const unread = this.buf.length - this.index;

      if (unread <= 0) {
        return null;
      }

      const toRead = Math.min(p.length, unread);

      p.set(this.buf.subarray(this.index, this.index + toRead));
      this.index += toRead;

      return toRead;
    }

    public reset() {
      this.index = 0;
    }
  }

  const text = new Array(1 * 64 * 1024 + 1).fill(0).map((_, i) => i % 10).join('');

  const reader = new CSVReader(new MyReader(text));
  
  reader.read();  
}
