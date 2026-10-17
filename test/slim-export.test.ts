import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import * as slim from "../src/slim.js";

describe("slim export", () => {
  it("declares the package subpath export", () => {
    const packageJson = JSON.parse(
      readFileSync(
        new URL("../package.json", import.meta.url),
        "utf8"
      )
    ) as {
      exports: Record<string, unknown>;
    };

    expect(packageJson.exports["./slim"]).toEqual({
      types: "./dist/slim.d.ts",
      import: "./dist/slim.js",
    });
  });

  it("omits ARK's wasm helper while keeping slim keyhive exports", () => {
    expect("initKeyhiveWasm" in slim).toBe(false);
    expect("isWasmInitialized" in slim).toBe(false);
    expect("initFromBase64Wasm" in slim).toBe(true);
    expect("initializeAutomergeRepoKeyhive" in slim).toBe(true);
  });
});
