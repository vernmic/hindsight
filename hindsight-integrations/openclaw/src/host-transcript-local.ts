// Local, read-only host-transcript reader for the Astinus classification pass.
//
// The prune-era assemble-marks.ts no longer exports HostTranscript, so the pass owns the
// small surface it needs: open(dbPath, sessionId), events(), anchorsFor(), seqBounds(),
// createdAt() and close(). Nothing here writes: the database is opened with readOnly and
// every method is a SELECT.
//
// Host transcript payload contract (OpenClaw v2026.10.3 / a20345f3
// src/config/sessions/transcript-payload.ts prepareTranscriptPayload + registerDecoder,
// src/infra/zstd-codec.ts): a row is either identity (event_json TEXT) or compressed
// (event_json NULL, event_zstd = one Zstd frame, event_utf8_bytes = original UTF-8 length).
// The decode copies the host's bounds and checks.
import { DatabaseSync } from "node:sqlite";
import * as zlib from "node:zlib";

const MAX_COMPRESSED_EVENT_BYTES = 4 * 1024 * 1024;
const utf8Decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

export type TranscriptRow = {
  seq: number;
  event_json: string | null;
  event_zstd?: unknown;
  event_utf8_bytes?: unknown;
};

export type EventRange = {
  descending?: boolean;
  lo?: number;
  hi?: number;
  limit?: number;
};

/** Decode one transcript row to its event JSON text; null when the row cannot be decoded. */
function decodeTranscriptEventJson(row: TranscriptRow): string | null {
  if (typeof row.event_json === "string") return row.event_json;
  const bytes = row.event_zstd;
  const rawBytes = row.event_utf8_bytes;
  if (bytes === null || bytes === undefined) return null;
  if (
    !(bytes instanceof Uint8Array) ||
    bytes.byteLength === 0 ||
    bytes.byteLength > MAX_COMPRESSED_EVENT_BYTES ||
    typeof rawBytes !== "number" ||
    !Number.isSafeInteger(rawBytes) ||
    rawBytes < 1 ||
    rawBytes > MAX_COMPRESSED_EVENT_BYTES
  ) {
    return null;
  }
  // Node >= 22.15 / 23.8 ship zlib Zstd; the installed @types/node predates it, hence the cast.
  const zstdDecompressSync = (
    zlib as unknown as {
      zstdDecompressSync?: (data: Uint8Array, options: { maxOutputLength: number }) => Buffer;
    }
  ).zstdDecompressSync;
  if (typeof zstdDecompressSync !== "function") return null;
  try {
    const decoded = zstdDecompressSync.call(zlib, bytes, { maxOutputLength: rawBytes });
    if (decoded.byteLength !== rawBytes) return null;
    return utf8Decoder.decode(decoded);
  } catch {
    return null;
  }
}

function eventIdOf(eventJson: string): string | null {
  try {
    const ev = JSON.parse(eventJson) as Record<string, unknown>;
    return typeof ev.id === "string" && ev.id ? ev.id : null;
  } catch {
    return null;
  }
}

/** Read-only view of one session's transcript_events rows. */
export class HostTranscript {
  private compressed: boolean | null = null;

  private constructor(
    private readonly db: DatabaseSync,
    readonly sessionId: string
  ) {}

  static open(dbPath: string, sessionId: string): HostTranscript {
    return new HostTranscript(new DatabaseSync(dbPath, { readOnly: true }), sessionId);
  }

  /** Undecoded rows in seq order (or reversed when descending). */
  rows(range: EventRange = {}): TranscriptRow[] {
    if (this.compressed === null) {
      const cols = this.db.prepare("PRAGMA table_info(transcript_events)").all() as Array<{
        name?: unknown;
      }>;
      const names = new Set(cols.map((col) => String(col.name)));
      this.compressed = names.has("event_zstd") && names.has("event_utf8_bytes");
    }
    const where = ["session_id = ?"];
    const args: Array<string | number> = [this.sessionId];
    for (const [op, v] of [
      ["seq >= ?", range.lo],
      ["seq <= ?", range.hi],
    ] as const) {
      if (v !== undefined) (where.push(op), args.push(v));
    }
    const cols = this.compressed
      ? "seq, event_json, event_zstd, event_utf8_bytes"
      : "seq, event_json";
    const limit = range.limit !== undefined ? (args.push(range.limit), " LIMIT ?") : "";
    return this.db
      .prepare(
        `SELECT ${cols} FROM transcript_events WHERE ${where.join(" AND ")} ORDER BY seq ${range.descending ? "DESC" : "ASC"}${limit}`
      )
      .all(...args) as TranscriptRow[];
  }

  /** Decoded events in the requested seq order; rows that cannot be decoded are skipped. */
  events(range: EventRange = {}): Array<{ seq: number; eventJson: string }> {
    const out: Array<{ seq: number; eventJson: string }> = [];
    for (const r of this.rows(range)) {
      const eventJson = decodeTranscriptEventJson(r);
      if (eventJson !== null) out.push({ seq: Number(r.seq), eventJson });
    }
    return out;
  }

  seqBounds(): { min: number | null; max: number | null } {
    const r = this.db
      .prepare("SELECT MIN(seq) AS lo, MAX(seq) AS hi FROM transcript_events WHERE session_id = ?")
      .get(this.sessionId) as { lo?: number | null; hi?: number | null } | undefined;
    return { min: r?.lo ?? null, max: r?.hi ?? null };
  }

  createdAt(seq: number): number | null {
    const r = this.db
      .prepare("SELECT created_at FROM transcript_events WHERE session_id = ? AND seq = ?")
      .get(this.sessionId, seq) as { created_at?: unknown } | undefined;
    return typeof r?.created_at === "number" ? r.created_at : null;
  }

  /** Host event ids of the first and last identified rows in [lo, hi]; null when none. */
  anchorsFor(lo: number, hi: number): { first: string; last: string } | null {
    let first: string | null = null;
    let last: string | null = null;
    for (const r of this.rows({ lo, hi })) {
      const eventJson = decodeTranscriptEventJson(r);
      if (eventJson === null) return null; // an undecodable row inside the range: do not guess
      const id = eventIdOf(eventJson);
      if (id) (first ??= id), (last = id);
    }
    return first && last ? { first, last } : null;
  }

  close(): void {
    try {
      this.db.close();
    } catch {
      /* already closed */
    }
  }
}
