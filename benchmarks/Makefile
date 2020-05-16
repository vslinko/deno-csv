test: test-deno-csv test-node-csv-parse

test-deno-csv: 500000-Records.csv
	deno run --allow-env --allow-read --allow-hrtime deno-csv.ts 500000-Records.csv

test-node-csv-parse: node_modules 500000-Records.csv
	node npm-csv-parse.js 500000-Records.csv

node_modules:
	npm ci

500000-Records.csv:
  wget http://eforexcel.com/wp/wp-content/uploads/2017/07/500000-Records.zip
	unzip 500000-Records.zip
	rm 500000-Records.zip
	mv "500000 Records.csv" 500000-Records.csv

.PHONY:	test test-deno-csv test-node-csv-parse
