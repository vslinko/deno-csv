all: test lint benchmark

test:
	deno test --allow-read reader_test.ts writer_test.ts csv_spectrum_test.ts

lint:
	deno fmt --check *.ts benchmarks/*.ts benchmarks/*.js

benchmark:
	cd benchmarks && make

test_wasm.ts: test.wasm
	deno run --allow-read=test.wasm convert.ts test.wasm > test_wasm.ts

test.wasm: test.wat
	wat2wasm test.wat

.PHONY: test lint benchmark
