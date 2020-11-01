run: test.wasm
	deno run --allow-read=test.wasm --v8-flags=--experimental-wasm-simd test.ts

test.wasm: test.wat
	wasm-opt test.wat --enable-simd -O4 -o test.wasm

.PHONY: run
