/**
 * Filesystem adapter for the two-turn capture store.
 *
 * The core rotation is pure; this adapter owns the bytes: one directory per
 * session key, one gzip JSON per turn, atomic rename on write, and a
 * content-addressed blob directory for request and response bodies.
 * The root is supplied by the caller; nothing here knows a host path.
 */

import { createHash } from "node:crypto";
import { mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { gzipSync, gunzipSync } from "node:zlib";

import type { CapturedTurn, TurnStore } from "./prompt-capture.js";

/** Filesystem- and URL-safe directory segment. Long or unsafe keys are hashed. */
export function safeSegment(value: string): string {
  const clean = value.replace(/[^a-zA-Z0-9._-]/g, "_");
  if (clean === value && clean.length > 0 && clean.length <= 96) return clean;
  return "h-" + createHash("sha256").update(value).digest("hex").slice(0, 32);
}

async function writeAtomic(path: string, data: Buffer): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const tmp = path + ".tmp-" + process.pid + "-" + Date.now();
  await writeFile(tmp, data);
  await rename(tmp, path);
}

export class FsTurnStore implements TurnStore {
  readonly root: string;

  constructor(root: string) {
    this.root = root;
  }

  private dir(sessionKey: string): string {
    return join(this.root, "turns", safeSegment(sessionKey));
  }

  private file(sessionKey: string, turnId: string): string {
    return join(this.dir(sessionKey), safeSegment(turnId) + ".json.gz");
  }

  async list(sessionKey: string): Promise<string[]> {
    try {
      const names = await readdir(this.dir(sessionKey));
      return names
        .filter((name) => name.endsWith(".json.gz"))
        .map((name) => name.slice(0, -".json.gz".length));
    } catch {
      return [];
    }
  }

  async read(sessionKey: string, turnId: string): Promise<CapturedTurn | null> {
    try {
      const raw = await readFile(this.file(sessionKey, turnId));
      return JSON.parse(gunzipSync(raw).toString("utf8")) as CapturedTurn;
    } catch {
      return null;
    }
  }

  async write(sessionKey: string, turn: CapturedTurn): Promise<void> {
    const body = gzipSync(Buffer.from(JSON.stringify(turn), "utf8"));
    await writeAtomic(this.file(sessionKey, turn.turnId), body);
  }

  async remove(sessionKey: string, turnId: string): Promise<void> {
    try {
      await rm(this.file(sessionKey, turnId), { force: true });
    } catch {
      // Missing is not an error: rotation may race a reader.
    }
  }
}

/** Content-addressed blob store for exact request and response bodies. */
export class FsBlobStore {
  readonly root: string;

  constructor(root: string) {
    this.root = root;
  }

  pathFor(sha256: string): string {
    return join(this.root, "blobs", sha256.slice(0, 2), sha256 + ".gz");
  }

  /** Store once; returns the hash. Identical bodies are not rewritten. */
  async put(bytes: Buffer): Promise<{ sha256: string; bytes: number }> {
    const sha256 = createHash("sha256").update(bytes).digest("hex");
    const path = this.pathFor(sha256);
    try {
      await readFile(path);
    } catch {
      await writeAtomic(path, gzipSync(bytes));
    }
    return { sha256, bytes: bytes.length };
  }

  async get(sha256: string): Promise<Buffer | null> {
    try {
      return gunzipSync(await readFile(this.pathFor(sha256)));
    } catch {
      return null;
    }
  }
}
