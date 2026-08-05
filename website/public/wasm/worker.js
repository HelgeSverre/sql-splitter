// Playground worker: runs sql-splitter's WASM module off the main thread.
// Protocol: see website/src/pages/playground.astro (the only client).
import init, { PlaygroundSession } from "./sql_splitter_wasm.js";

const ready = init();
let session = null;

self.onmessage = async (event) => {
  const { id, type } = event.data;
  try {
    await ready;
    if (type === "analyze") {
      self.postMessage({ id, type: "status", stage: "analyzing" });
      if (session) {
        session.free();
        session = null;
      }
      const bytes = new Uint8Array(event.data.buffer);
      session = new PlaygroundSession(bytes, event.data.dialect ?? undefined);
      const summary = JSON.parse(session.summary());
      self.postMessage({ id, type: "analyzed", summary });
    } else if (type === "generate") {
      if (!session) throw new Error("No dump analyzed yet");
      self.postMessage({ id, type: "status", stage: "generating" });
      const sql = session.generate(event.data.rows, event.data.seed);
      self.postMessage({ id, type: "generated", sql });
    }
  } catch (err) {
    // A WebAssembly trap (panic under panic=abort) leaves the instance's
    // memory untrusted; the page terminates and respawns us on fatal errors.
    const fatal = err instanceof WebAssembly.RuntimeError;
    if (fatal) session = null;
    self.postMessage({
      id,
      type: "error",
      message: err?.message ?? String(err),
      fatal,
    });
  }
};
