/**
 * Version is injected at build time via Vite's define feature.
 * See astro.config.mjs for the getVersion() function.
 */
declare const __SQL_SPLITTER_VERSION__: string;

export const VERSION =
  typeof __SQL_SPLITTER_VERSION__ !== "undefined"
    ? __SQL_SPLITTER_VERSION__
    : "0.0.0";
