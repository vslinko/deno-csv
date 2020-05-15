import { hasPrefix, getLogger } from "./deps.ts";

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
