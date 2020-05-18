all: test lint benchmark

test:
	deno test --allow-read reader_test.ts writer_test.ts csv_spectrum_test.ts

lint:
	deno fmt --check *.ts benchmarks/*.ts benchmarks/*.js

benchmark:
	cd benchmarks && make

.PHONY: test lint benchmark
