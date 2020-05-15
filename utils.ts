import { hasPrefix, getLogger } from "./deps.ts";

const enc = new TextEncoder();

export function getUint8Array(str: string | Uint8Array): Uint8Array {
  return str instanceof Uint8Array ? str : enc.encode(str);
}

export function hasPrefixFrom(
  a: Uint8Array,
  prefix: Uint8Array,
  offset: number,
) {
  return hasPrefix(offset > 0 ? a.subarray(offset) : a, prefix);
}

export async function* dummyAsyncIterable(
  str: Uint8Array,
): AsyncIterableIterator<Uint8Array> {
  yield str;
}

export function isAsyncIterable(iter: any): iter is AsyncIterable<any> {
  return !!iter[Symbol.asyncIterator];
}

export type SyncAsyncIterable<T> = AsyncIterable<T> | Iterable<T>;

export async function* makeAsyncIterable<T>(
  iter: SyncAsyncIterable<T>,
): AsyncIterableIterator<T> {
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
  iter: AsyncIterableIterator<T>,
): Promise<Array<T>> {
  const arr: T[] = [];
  for await (const row of iter) {
    arr.push(row);
  }
  return arr;
}

export async function asyncArrayFrom2<T>(iter: AsyncIterableIterator<AsyncIterableIterator<T>>): Promise<T[][]> {
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
