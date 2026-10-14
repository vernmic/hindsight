"""Carry earlier units' tags across an append that falls back to full re-extraction.

``update_mode="append"`` may only ADD. The delta path honours that by leaving unchanged chunks'
units (and their tags) alone, but an append can still reach the full path: the chunker
re-balances the boundaries so no chunk survives unchanged, source-context extraction disables
the delta, or ``force_reextract`` skips it. That path deletes every old unit and re-extracts the
whole document, and the new units carry only the append request's tags, so every per-unit
classification (``retain:*``, ``kind:*``, ``tier:*`` ... written by a tagger after extraction)
would be lost.

:class:`AppendTagCarry` is built inside the write transaction, from a snapshot of the outgoing
units taken just before they are deleted, and assigns each re-extracted unit its tags:

1. **Exact text** (whitespace-normalised, case-sensitive): a new unit whose text equals one or
   more old units' text takes those units' tags.
2. **Genuinely new text**: a unit extracted from a chunk that lies wholly after the pre-append
   text keeps the request's tags only.
3. **Pre-append text**: the old units of the old chunk(s) that overlap the new chunk's span
   (positions in the pre-append text) are the candidates. The candidate with the clearly best
   word overlap (Jaccard >= 0.5; ties all count) gives its tags; otherwise the union of all
   candidates' tags is used, so a tag is never lost but may be shared by siblings of one old
   chunk.
4. **Straddling chunk** (holds both pre-append and appended text) with no clear text match:
   the request's tags plus the candidates' union, since the unit may stem from either part.
5. **Position unknown** (a chunk not found verbatim in the text, e.g. a JSON-array merge whose
   body is not a plain prefix extension): the best Jaccard match over all old units if one
   clears 0.5, else the request's tags plus the union of every old unit's tags, the
   conservative choice: no earlier tag is lost.
6. **Orphans** (:meth:`AppendTagCarry.assign`, per batch): an old unit overlapping a new chunk
   that no re-extracted unit matched (its content was folded into a sibling, e.g. one new chunk
   now spans two old ones) hands its tags to every unit of that new chunk, so the document as a
   whole keeps every tag.

Inherited tags exclude entity-label projections (``label_tag_keys``): those are derived from
the unit's own entities, so the new unit keeps the label tags its fresh extraction produced.
Unconditional: it is append semantics, like the delta-path survivor guarantee.

Limits: the mapping is heuristic where one old chunk produced several differently-tagged
units and the re-extracted text matches none of them clearly (they then share the union), and
orphan tags spread to every unit of the new chunk. Tags may over-spread within the overlapping
old/new chunks; they never reach units of genuinely new text or of unrelated chunks, and none
is dropped. A crash between streaming batches (after the old units are deleted) loses the
snapshot for later batches, as the replace itself does. Old observations are invalidated by
the normal document-replace path and re-consolidated from the new units.
Store-owned banks (no SQL chunk rows) only get exact-text and Jaccard matching plus the
conservative union.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

_WORD = re.compile(r"\w+", re.UNICODE)
JACCARD_MIN = 0.5


@dataclass(frozen=True)
class OldUnit:
    text: str
    fact_type: str
    tags: tuple[str, ...]
    chunk_index: int | None = None


@dataclass
class OldDocumentSnapshot:
    units: list[OldUnit] = field(default_factory=list)
    # chunk_index -> chunk text, for the outgoing version's chunks.
    chunks: dict[int, str] = field(default_factory=dict)


def _norm(text: str | None) -> str:
    return " ".join((text or "").split())


def _words(text: str | None) -> frozenset[str]:
    return frozenset(w.lower() for w in _WORD.findall(text or ""))


def _jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def locate_spans(pieces: Sequence[str], text: str) -> list[tuple[int, int] | None]:
    """Each piece's ``(start, end)`` in ``text``, searched in order; None when not found verbatim.

    Chunks are cut in order, so the search resumes at the previous piece's start (pieces may
    overlap) and falls back to a search from the beginning.
    """
    spans: list[tuple[int, int] | None] = []
    cursor = 0
    for piece in pieces:
        if not piece:
            spans.append(None)
            continue
        pos = text.find(piece, cursor)
        if pos < 0:
            pos = text.find(piece)
        if pos < 0:
            spans.append(None)
            continue
        spans.append((pos, pos + len(piece)))
        cursor = pos
    return spans


def new_chunk_spans(base_text: str | None, combined_text: str, new_chunks: Sequence[str]):
    """Where each new chunk sits in the new body, or None when positions are unusable (the new
    body does not extend ``base_text`` verbatim). Computed up front: the streaming retain frees
    chunk strings as it extracts them."""
    if not base_text or not combined_text.startswith(base_text):
        return None
    return locate_spans(new_chunks, combined_text)


def _union(tag_lists: Iterable[Iterable[str]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for tags in tag_lists:
        for t in tags:
            if t not in seen:
                seen.add(t)
                out.append(t)
    return out


class AppendTagCarry:
    def __init__(
        self,
        *,
        base_text: str,
        new_spans: list[tuple[int, int] | None] | None,
        snapshot: OldDocumentSnapshot,
        label_tag_keys: set[str] | None = None,
    ) -> None:
        from .entity_labels import split_label_tags

        self._split_labels = lambda tags: split_label_tags(list(tags), label_tag_keys)
        self._units = snapshot.units
        self._base_len = len(base_text)
        # None: positions unusable (the new body does not extend the stored one verbatim).
        self._new_spans = new_spans or []
        ordered = sorted(snapshot.chunks.items())
        old_spans = locate_spans([t for _, t in ordered], base_text)
        self._old_chunk_spans = {idx: span for (idx, _), span in zip(ordered, old_spans, strict=True)}
        self._by_text: dict[str, list[OldUnit]] = {}
        for u in self._units:
            self._by_text.setdefault(_norm(u.text), []).append(u)
        self._words = {id(u): _words(u.text) for u in self._units}

    @property
    def has_units(self) -> bool:
        return bool(self._units)

    def _inheritable(self, units: Iterable[OldUnit]) -> list[str]:
        labels = set()
        merged = _union(u.tags for u in units)
        labels = set(self._split_labels(merged))
        return [t for t in merged if t not in labels]

    def _best(self, text: str, candidates: Sequence[OldUnit]) -> list[OldUnit]:
        words = _words(text)
        scored = [(_jaccard(words, self._words[id(u)]), u) for u in candidates]
        if not scored:
            return []
        top = max(s for s, _ in scored)
        if top < JACCARD_MIN:
            return []
        return [u for s, u in scored if s == top]

    def _candidates(self, span: tuple[int, int]) -> list[OldUnit]:
        start, end = span[0], min(span[1], self._base_len)
        chunks = {
            idx
            for idx, ospan in self._old_chunk_spans.items()
            if ospan is not None and ospan[0] < end and start < ospan[1]
        }
        found = [u for u in self._units if u.chunk_index in chunks]
        return found or list(self._units)

    def _match(self, fact_text: str, chunk_position: int | None, current: list[str]):
        """``(tags, matched old units, candidate old units)`` for one re-extracted unit."""
        own_labels = self._split_labels(current)

        def _old(units: Sequence[OldUnit]) -> list[str]:
            return _union([self._inheritable(units), own_labels])

        span = None
        if chunk_position is not None and 0 <= chunk_position < len(self._new_spans):
            span = self._new_spans[chunk_position]
        if span is not None and span[0] >= self._base_len:
            candidates: list[OldUnit] = []
        elif span is not None:
            candidates = self._candidates(span)
        else:
            candidates = list(self._units)

        exact = self._by_text.get(_norm(fact_text))
        if exact:
            return _old(exact), exact, candidates
        if span is not None and span[0] >= self._base_len:
            return current, [], candidates  # genuinely new text
        best = self._best(fact_text, candidates)
        if best:
            return _old(best), best, candidates
        if span is None or span[1] > self._base_len:
            # Position unknown, or the chunk also holds appended text: it may stem from either.
            return _union([current, self._inheritable(candidates)]), list(candidates), candidates
        return _old(candidates), list(candidates), candidates

    def tags_for(self, fact_text: str, chunk_position: int | None, current_tags: Sequence[str]) -> list[str]:
        """The tags one re-extracted unit should carry (rules 1-5 of the module docstring).

        ``chunk_position`` indexes the new chunks; ``current_tags`` is what extraction gave it
        (the request's tags plus its own label projection).
        """
        current = list(current_tags or [])
        if not self._units:
            return current
        return self._match(fact_text, chunk_position, current)[0]

    def assign(self, facts: Sequence[tuple[str, int | None, Sequence[str]]]) -> list[list[str]]:
        """Tags for a batch of re-extracted units ``(text, chunk position, current tags)``.

        Per :meth:`tags_for`, plus rule 6: an old unit that is a candidate of a new chunk but that
        none of the batch's units matched (the re-extraction folded its content into a sibling,
        e.g. one new chunk now spans two old ones) is an orphan; its tags go to every unit of
        the new chunk(s) it overlaps, so the document never loses a tag. A batch holds whole
        chunks, so a chunk's units are always judged together.
        """
        if not self._units:
            return [list(c or []) for _, _, c in facts]
        results = []
        matched: set[int] = set()
        by_chunk: dict[int | None, list[int]] = {}
        chunk_candidates: dict[int | None, list[OldUnit]] = {}
        for i, (text, pos, current) in enumerate(facts):
            tags, hit, candidates = self._match(text, pos, list(current or []))
            results.append(tags)
            matched.update(id(u) for u in hit)
            by_chunk.setdefault(pos, []).append(i)
            chunk_candidates[pos] = candidates
        for pos, members in by_chunk.items():
            orphans = [u for u in chunk_candidates[pos] if id(u) not in matched]
            if not orphans or pos is None:
                continue
            extra = self._inheritable(orphans)
            for i in members:
                results[i] = _union([results[i], extra])
        return results
