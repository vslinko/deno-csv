const wasmCode = Deno.readFileSync(Deno.args[0]);

const tsFile = `export const wasmCode = new Uint8Array([
  ${wasmCode.join(",")}
]);`;

console.log(tsFile);
