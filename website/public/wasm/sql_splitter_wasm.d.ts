/* tslint:disable */
/* eslint-disable */

export class PlaygroundSession {
  free(): void;
  [Symbol.dispose](): void;
  /**
   * The resolved `kind: model` YAML document for the current inference —
   * the same document the CLI emits with `--emit-config`, with row counts
   * frozen, inference disabled, and the seed pinned.
   */
  modelYaml(
    rows: number,
    seed: number,
    dialect?: string | null,
    mode?: string | null,
  ): string;
  /**
   * Warnings raised by the most recent `generate` call (compile
   * diagnostics + cross-dialect conversion losses), as a JSON array.
   */
  lastRenderWarnings(): string;
  /**
   * Profile a dump and infer a generation model from it.
   *
   * `dialect` is `"mysql" | "postgres" | "sqlite" | "mssql"`, or `None` to
   * sniff the first 8KB. `on_progress`, when given, is called with a
   * `0.0..=1.0` fraction as the profiler consumes the dump.
   */
  constructor(
    dump: Uint8Array,
    dialect?: string | null,
    on_progress?: Function | null,
  );
  /**
   * The analyze summary as a JSON string (see `Summary` for the shape).
   */
  summary(): string;
  /**
   * Render synthetic SQL. `rows` is the row count for root tables (children
   * derive their counts from relationships); `seed` makes output
   * deterministic. `dialect` selects the output dialect (default: the
   * source dialect); `mode` is `schema_and_data | schema_only | data_only`.
   */
  generate(
    rows: number,
    seed: number,
    dialect?: string | null,
    mode?: string | null,
  ): string;
}

export function start(): void;

export type InitInput =
  RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly __wbg_playgroundsession_free: (a: number, b: number) => void;
  readonly playgroundsession_generate: (
    a: number,
    b: number,
    c: number,
    d: number,
    e: number,
    f: number,
    g: number,
    h: number,
  ) => void;
  readonly playgroundsession_lastRenderWarnings: (a: number, b: number) => void;
  readonly playgroundsession_modelYaml: (
    a: number,
    b: number,
    c: number,
    d: number,
    e: number,
    f: number,
    g: number,
    h: number,
  ) => void;
  readonly playgroundsession_new: (
    a: number,
    b: number,
    c: number,
    d: number,
    e: number,
    f: number,
  ) => void;
  readonly playgroundsession_summary: (a: number, b: number) => void;
  readonly start: () => void;
  readonly __wbindgen_export: (a: number) => void;
  readonly __wbindgen_export2: (a: number, b: number, c: number) => void;
  readonly __wbindgen_export3: (a: number, b: number) => number;
  readonly __wbindgen_export4: (
    a: number,
    b: number,
    c: number,
    d: number,
  ) => number;
  readonly __wbindgen_add_to_stack_pointer: (a: number) => number;
  readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(
  module: { module: SyncInitInput } | SyncInitInput,
): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init(
  module_or_path?:
    | { module_or_path: InitInput | Promise<InitInput> }
    | InitInput
    | Promise<InitInput>,
): Promise<InitOutput>;
