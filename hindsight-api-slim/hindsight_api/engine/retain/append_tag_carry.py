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

5. **Retag marker** (:meth:`AppendTagCarry.assign_detailed`): an old unit that carried
   memory-only tags (tags beyond the request's and beyond those every old unit shares) and that
   no re-extracted unit matched is an orphan. A re-extracted unit with no confident match that
   plausibly replaces an orphan gets ``retag:pending`` plus one ``retag-candidate:<tag>`` per
   orphan memory-only tag (capped). "Plausibly" means: its chunk lies in pre-append text and
   overlaps the orphan's old chunk, or (positions unknown) its word overlap with the orphan is
   at least ``PARTIAL_MIN``. Genuinely new content (a chunk wholly after the pre-append text, or
   no partial overlap with any orphan) is never marked. Candidates are hints for the retag
   pass only: no ``retain:``/``kind:`` value is applied, the ``retag-candidate:`` prefix never
   equals a real tag in a filter or a protected-tag check, and a tagger removes the
   ``retag:*``/``retag-candidate:*`` tags once it has classified the unit.

Limits: an old unit whose re-extracted text drifts below the similarity threshold (or whose
content is folded into a sibling) loses its unit-only tags; its plausible replacement is
marked for retagging instead (rule 5). That is the deliberate trade against over-spreading.
Orphans are judged per streaming batch (a batch holds whole chunks). A crash between streaming batches
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
PARTIAL_MIN = 0.3
RETAG_MARKER = "retag:pending"
RETAG_CANDIDATE_PREFIX = "retag-candidate:"
RETAG_CANDIDATE_CAP = 16


def is_retag_tag(tag: str) -> bool:
    return tag == RETAG_MARKER or tag.startswith(RETAG_CANDIDATE_PREFIX) or tag.startswith("retag:")


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
        # Tags every old unit shares (document-level): not memory-only. One unit: nothing shared.
        sets = [set(self._inheritable([u])) for u in self._units]
        self._common = set.intersection(*sets) if len(sets) > 1 else set()

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

    def _memory_only(self, unit: OldUnit, current: Sequence[str]) -> list[str]:
        cur = set(current)
        return [
            t
            for t in self._inheritable([unit])
            if t not in cur and t not in self._common and not is_retag_tag(t)
        ]

    def assign(self, facts: Sequence[tuple[str, int | None, Sequence[str]]]) -> list[list[str]]:
        """Tags for a batch of re-extracted units ``(text, chunk position, current tags)``.

        Matched units inherit exactly their old unit's tags; everything else keeps its own. No
        orphan spreading: an unmatched old unit's tags go to nobody - its plausible replacement
        is only marked for retagging (rule 5)."""
        return self.assign_detailed(facts)[0]

    def assign_detailed(self, facts):
        """``(tags per fact, retag info per fact)``; retag info is None or
        ``{"candidates": [...], "orphans": n}`` for a marked unit."""
        current_lists = [list(c or []) for _, _, c in facts]
        if not self._units:
            return current_lists, [None] * len(facts)
        tags_out, hits, spans = [], [], []
        matched: set[int] = set()
        for (text, pos, _), current in zip(facts, current_lists):
            tags, hit = self._match(text, pos, current)
            tags_out.append(tags)
            hits.append(hit)
            matched.update(id(u) for u in hit)
            span = self._new_spans[pos] if pos is not None and 0 <= pos < len(self._new_spans) else None
            spans.append(span)
        info: list[dict | None] = [None] * len(facts)
        for i, ((text, _, _), current) in enumerate(zip(facts, current_lists)):
            if hits[i]:
                continue
            span = spans[i]
            if span is not None and span[0] >= self._base_len:
                continue  # genuinely new content: never marked
            if span is not None:
                pool = [(u, None) for u in self._candidates(span)]
            else:
                words = _words(text)
                pool = [(u, _jaccard(words, self._words[id(u)])) for u in self._units]
                pool = [(u, s) for u, s in pool if s >= PARTIAL_MIN]
            orphans = [(u, s) for u, s in pool if id(u) not in matched and self._memory_only(u, current)]
            if not orphans:
                continue
            orphans.sort(key=lambda us: -(us[1] or 0.0))
            cands = _union(self._memory_only(u, current) for u, _ in orphans)[:RETAG_CANDIDATE_CAP]
            extra = [RETAG_MARKER] + [RETAG_CANDIDATE_PREFIX + t for t in cands]
            tags_out[i] = _union([tags_out[i], extra])
            info[i] = {"candidates": cands, "orphans": len(orphans)}
        return tags_out, info
