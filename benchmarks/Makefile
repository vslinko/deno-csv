test: test-node-csv-parse test-deno-csv-CSVReader test-deno-csv-readCSVRows test-deno-csv-readCSVStream test-deno-csv-readCSV

test-node-csv-parse: node_modules 500000-Records.csv
	@echo test-node-csv-parse
	@node npm-csv-parse.js 500000-Records.csv

test-deno-csv-CSVReader: 500000-Records.csv
	@echo test-deno-csv-CSVReader
	@deno run --allow-env --allow-read --allow-hrtime deno-csv-CSVReader.ts 500000-Records.csv

test-deno-csv-readCSVRows: 500000-Records.csv
	@echo test-deno-csv-readCSVRows
	@deno run --allow-env --allow-read --allow-hrtime deno-csv-readCSVRows.ts 500000-Records.csv

test-deno-csv-readCSVStream: 500000-Records.csv
	@echo test-deno-csv-readCSVStream
	@deno run --allow-env --allow-read --allow-hrtime deno-csv-readCSVStream.ts 500000-Records.csv

test-deno-csv-readCSV: 500000-Records.csv
	@echo test-deno-csv-readCSV
	@deno run --allow-env --allow-read --allow-hrtime deno-csv-readCSV.ts 500000-Records.csv

node_modules:
	npm ci

500000-Records.csv:
	wget http://eforexcel.com/wp/wp-content/uploads/2017/07/500000-Records.zip
	unzip 500000-Records.zip
	rm 500000-Records.zip
	mv "500000 Records.csv" 500000-Records.csv

.PHONY:	test test-node-csv-parse test-deno-csv-CSVReader test-deno-csv-readCSVRows test-deno-csv-readCSVStream test-deno-csv-readCSV
