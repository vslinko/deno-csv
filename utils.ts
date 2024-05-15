import { getLogger } from "@std/log";

const enc = new TextEncoder();

export function getUint8Array(str: string | Uint8Array): Uint8Array {
  return str instanceof Uint8Array ? str : enc.encode(str);
}

export function hasPrefixFrom(
  a: Uint8Array,
  prefix: Uint8Array,
  offset: number,
) {
  for (let i = 0, max = prefix.length; i < max; i++) {
    if (a[i + offset] !== prefix[i]) return false;
  }
  return true;
}

export async function* dummyAsyncIterable(
  str: Uint8Array,
): AsyncIterable<Uint8Array> {
  yield str;
}

// deno-lint-ignore no-explicit-any
export function isAsyncIterable(iter: any): iter is AsyncIterable<any> {
  return !!iter[Symbol.asyncIterator];
}

export type SyncAsyncIterable<T> = AsyncIterable<T> | Iterable<T>;

export async function* makeAsyncIterable<T>(
  iter: SyncAsyncIterable<T>,
): AsyncIterable<T> {
  const i = isAsyncIterable(iter)
    ? iter[Symbol.asyncIterator]()
    : iter[Symbol.iterator]();

  while (true) {
    const { done, value } = await i.next();
    if (done) {
      return;
    } else {
      yield value;
    }
  }
}

export function debug(msg: string) {
  getLogger("csv").debug(msg);
}

export async function asyncArrayFrom<T>(
  iter: AsyncIterable<T>,
): Promise<Array<T>> {
  const arr: T[] = [];
  for await (const row of iter) {
    arr.push(row);
  }
  return arr;
}

export async function asyncArrayFrom2<T>(
  iter: AsyncIterable<AsyncIterable<T>>,
): Promise<T[][]> {
  const arr: T[][] = [];
  for await (const rowIter of iter) {
    const row: T[] = [];
    for await (const cell of rowIter) {
      row.push(cell);
    }
    arr.push(row);
  }
  return arr;
}
