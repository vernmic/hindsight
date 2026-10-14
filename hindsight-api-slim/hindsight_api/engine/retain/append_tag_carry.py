"""Carry earlier units' tags across an append that falls back to full re-extraction.

``update_mode="append"`` may only ADD. The delta path honours that by leaving unchanged chunks'
units (and their tags) alone, but an append can still reach the full path: the chunker
re-balances the boundaries so no chunk survives unchanged, source-context extraction disables
the delta, or ``force_reextract`` skips it. That path deletes every old unit and re-extracts the
whole document, and the new units carry only the append request's tags, so every per-unit
classification (``retain:*``, ``kind:*``, ``tier:*`` ... written by a tagger after extraction)
would be lost.

:class:`AppendTagCarry` is built inside the write transaction, from a snapshot of the outgoing
units taken just before they are deleted, and assigns each re-extracted unit its tags. The rule
is precision first: a re-extracted unit inherits ONLY the tags of the one old unit it replaces,
and a unit with no confident match keeps exactly what extraction gave it (the request's tags,
none when the request omitted them or sent ``[]``). Earlier units' tags are never unioned onto
other units: spreading one fact's ``retain:permanent`` / ``kind:decision`` / ``task:`` labels to
its siblings or to genuinely new facts corrupts maintenance protection and filtered recall.

1. **Exact text** (whitespace-normalised, case-sensitive): a new unit whose text equals an old
   unit's text takes that unit's tags (several identical old units: only when they agree).
2. **Genuinely new text**: a unit extracted from a chunk that lies wholly after the pre-append
   text keeps the request's tags only.
3. **Similar text**: otherwise the old unit with the best word overlap (Jaccard >= 0.5) gives
   its tags. Candidates are the old units of the overlapping old chunk(s) when positions are
   known, else every old unit (e.g. a JSON-array merge whose body is not a plain prefix
   extension). A tie between old units with different tags is ambiguous: no carry.
4. **Anything else** keeps the request's tags only.

Inherited tags exclude entity-label projections (``label_tag_keys``): those are derived from
the unit's own entities, so the new unit keeps the label tags its fresh extraction produced.
Unconditional: it is append semantics, like the delta-path survivor guarantee.

Limits: an old unit whose re-extracted text drifts below the similarity threshold (or whose
content is folded into a sibling) loses its unit-only tags; a tagger re-classifies the new
unit. That is the deliberate trade against over-spreading. A crash between streaming batches
(after the old units are deleted) loses the snapshot for later batches, as the replace itself
does. Old observations are invalidated by the normal document-replace path and re-consolidated
from the new units.
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

    def _agreed(self, units: Sequence[OldUnit]) -> list[OldUnit]:
        """``units`` when their inheritable tags agree (one unambiguous source), else []."""
        if not units:
            return []
        first = set(self._inheritable(units[:1]))
        return list(units) if all(set(self._inheritable([u])) == first for u in units[1:]) else []

    def _match(self, fact_text: str, chunk_position: int | None, current: list[str]):
        """``(tags, matched old units)`` for one re-extracted unit (rules 1-4)."""
        own_labels = self._split_labels(current)
        span = None
        if chunk_position is not None and 0 <= chunk_position < len(self._new_spans):
            span = self._new_spans[chunk_position]

        hit = self._agreed(self._by_text.get(_norm(fact_text)) or [])
        if not hit:
            if span is not None and span[0] >= self._base_len:
                return current, []  # genuinely new text
            candidates = self._candidates(span) if span is not None else list(self._units)
            hit = self._agreed(self._best(fact_text, candidates))
        if not hit:
            return current, []
        return _union([self._inheritable(hit), own_labels]), hit

    def tags_for(self, fact_text: str, chunk_position: int | None, current_tags: Sequence[str]) -> list[str]:
        """The tags one re-extracted unit should carry (rules 1-4 of the module docstring).

        ``chunk_position`` indexes the new chunks; ``current_tags`` is what extraction gave it
        (the request's tags plus its own label projection).
        """
        current = list(current_tags or [])
        if not self._units:
            return current
        return self._match(fact_text, chunk_position, current)[0]

    def assign(self, facts: Sequence[tuple[str, int | None, Sequence[str]]]) -> list[list[str]]:
        """Tags for a batch of re-extracted units ``(text, chunk position, current tags)``, each
        judged independently by :meth:`tags_for`. No orphan spreading: an old unit no new unit
        matched hands its tags to nobody."""
        return [self.tags_for(text, pos, current) for text, pos, current in facts]
