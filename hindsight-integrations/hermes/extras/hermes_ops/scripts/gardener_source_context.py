"""Explicit whole-block source windows; originals remain in frozen audit snapshots."""

import hashlib
import re


def blocks_and_headings(text):
    blocks = []
    headings = []
    fence = None
    start = None
    offset = 0
    end = 0
    for line in text.splitlines(keepends=True):
        if fence is None:
            heading = re.match(r"^ {0,3}(#{1,6})\s+(.+)", line)
            if heading:
                headings.append({"offset": offset, "level": len(heading[1]), "text": line.rstrip("\r\n")})
        if not line.strip() and fence is None:
            if start is not None:
                blocks.append((start, end))
                start = None
        else:
            if start is None:
                start = offset
            marker = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)", line)
            if marker:
                if fence is None:
                    fence = (marker[1][0], len(marker[1]))
                elif marker[1][0] == fence[0] and len(marker[1]) >= fence[1] and not marker[2].strip():
                    fence = None
            end = offset + len(line)
        offset += len(line)
    if start is not None:
        blocks.append((start, end))
    return blocks, headings


def select_document(document, sources, full_limit=32000, radius=8000):
    text = document.get("text") or ""
    if len(text) <= full_limit:
        return document
    anchors = []
    for source in sources:
        if source.get("document_id") != document["document_id"]:
            continue
        target = source.get("text") or ""
        if not target:
            continue
        start = text.find(target)
        # Never guess which repeated passage or mismatched normalization is original.
        if start < 0 or text.find(target, start + 1) >= 0:
            return document
        anchors.append((start, start + len(target)))
    if not anchors:
        return document
    blocks, headings = blocks_and_headings(text)
    intervals = []
    for start, end in anchors:
        selected = [b for b in blocks if b[1] > max(0, start - radius) and b[0] < min(len(text), end + radius)]
        if not selected:
            return document
        intervals.append((selected[0][0], selected[-1][1]))
    merged = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    excerpts = []
    for start, end in merged:
        stack = []
        for heading in headings:
            if heading["offset"] > start:
                break
            stack = [h for h in stack if h["level"] < heading["level"]] + [heading]
        excerpts.append({"start": start, "end": end, "heading_context": stack, "text": text[start:end]})
    result = {k: v for k, v in document.items() if k != "text"}
    result.update(
        excerpts=excerpts,
        full_text_chars=len(text),
        full_text_sha256=hashlib.sha256(text.encode()).hexdigest(),
        evidence_scope="Selected whole paragraphs and complete fenced blocks around exact uniquely matched source chunks, with ancestor headings. Omitted document content is not evidence. Choose missing-context if referents, applicability or status require omitted context. The full original remains in the frozen audit input.",
    )
    return result
