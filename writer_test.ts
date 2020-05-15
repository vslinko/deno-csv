import { assertEquals } from "./dev_deps.ts";
import { CSVWriter, writeCSV, writeCSVObjects } from "./writer.ts";

Deno.test({
  name: "CSVWriter writes simple file",
  async fn() {
    const buf = new Deno.Buffer();
    const writer = new CSVWriter(buf);

    await writer.writeCell("a");
    await writer.writeCell("b");
    await writer.writeCell("c");
    await writer.nextLine();
    await writer.writeCell("1");
    await writer.writeCell("2");
    await writer.writeCell("3");

    assertEquals(new TextDecoder().decode(buf.bytes()), "a,b,c\n1,2,3");
  },
});

Deno.test({
  name: "CSVWriter detects quotes",
  async fn() {
    const buf = new Deno.Buffer();
    const writer = new CSVWriter(buf);

    await writer.writeCell("a");
    await writer.writeCell("b");
    await writer.writeCell("c");
    await writer.nextLine();
    await writer.writeCell('1"2');
    await writer.writeCell("2,3");
    await writer.writeCell("3\n4");

    assertEquals(
      new TextDecoder().decode(buf.bytes()),
      `a,b,c\n"1""2","2,3","3\n4"`,
    );
  },
});

Deno.test({
  name: "CSVWriter works with async iterable",
  async fn() {
    const buf = new Deno.Buffer();
    const writer = new CSVWriter(buf);
    const asyncCell = async function* () {
      const enc = new TextEncoder();
      yield enc.encode("1");
      yield enc.encode("\n");
      yield enc.encode('"');
      yield enc.encode(",");
      yield enc.encode("2");
    };

    await writer.writeCell("a");
    await writer.nextLine();
    await writer.writeCell(asyncCell());

    assertEquals(new TextDecoder().decode(buf.bytes()), `a\n"1\n"",2"`);
  },
});

Deno.test({
  name: "writeCSV works with different input",
  async fn() {
    const buf = new Deno.Buffer();
    const enc = new TextEncoder();
    const asyncCell = async function* (str: string) {
      yield enc.encode(str);
    };
    const asyncRow = async function* () {
      yield "1";
      yield "2";
      yield asyncCell("3");
    };
    const asyncRows = async function* () {
      yield ["a", "b", asyncCell("c")];
      yield asyncRow();
    };

    await writeCSV(buf, asyncRows());

    assertEquals(new TextDecoder().decode(buf.bytes()), `a,b,"c"\n1,2,"3"`);
  },
});

Deno.test({
  name: "writeCSVObjects works objects",
  async fn() {
    const buf = new Deno.Buffer();
    const asyncRows = async function* () {
      yield { a: "1", b: "2", c: "3" };
      yield { a: "4", b: "5", c: "6" };
    };

    await writeCSVObjects(buf, asyncRows(), { header: ["a", "b", "c"] });

    assertEquals(new TextDecoder().decode(buf.bytes()), `a,b,c\n1,2,3\n4,5,6`);
  },
});
