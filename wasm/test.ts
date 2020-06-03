const wasmCode = Deno.readFileSync("test.wasm");
const wasmModule = new WebAssembly.Module(wasmCode);
const memory = new WebAssembly.Memory({
  initial: 3,
  maximum: 10,
});
const instance = new WebAssembly.Instance(wasmModule, {
  main: {
    memory,
  }
});

console.log(instance.exports.x());
