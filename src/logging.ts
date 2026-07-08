/**
 * Minimal leveled logger for the library. Defaults to "warn" so importing
 * or running ARK is quiet apart from warnings and errors. Applications can
 * raise the level during debugging:
 *
 * ```ts
 * import { setKeyhiveLogLevel } from "@automerge/automerge-repo-keyhive";
 * setKeyhiveLogLevel("debug");
 * ```
 */
export type KeyhiveLogLevel = "silent" | "error" | "warn" | "info" | "debug";

const LEVEL_ORDER: Record<KeyhiveLogLevel, number> = {
  silent: 0,
  error: 1,
  warn: 2,
  info: 3,
  debug: 4,
};

let currentLevel: KeyhiveLogLevel = "warn";

export function setKeyhiveLogLevel(level: KeyhiveLogLevel): void {
  currentLevel = level;
}

export function getKeyhiveLogLevel(): KeyhiveLogLevel {
  return currentLevel;
}

function enabled(level: KeyhiveLogLevel): boolean {
  return LEVEL_ORDER[currentLevel] >= LEVEL_ORDER[level];
}

export const log = {
  error(...args: unknown[]): void {
    if (enabled("error")) console.error(...args);
  },
  warn(...args: unknown[]): void {
    if (enabled("warn")) console.warn(...args);
  },
  info(...args: unknown[]): void {
    if (enabled("info")) console.log(...args);
  },
  debug(...args: unknown[]): void {
    if (enabled("debug")) console.debug(...args);
  },
};
