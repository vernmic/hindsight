import json
from types import SimpleNamespace


def _minimal_config(**overrides):
    values = dict(
        retain_extraction_mode="concise",
        retain_extract_causal_links=True,
        retain_custom_instructions=None,
        retain_mission=None,
        entity_labels=None,
        entities_allow_free_form=True,
        llm_output_language=None,
        llm_supports_string_pattern=False,
        retain_optional_fact_dimensions=False,
        retain_chunk_size=50,
        retain_structured_chunk_size=None,
        retain_max_attachments_per_chunk=8,
        retain_batch_enabled=False,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


from hindsight_api.engine.retain.fact_extraction import build_chunk_prompt_parts
from hindsight_api.engine.retain.source_context import MARKER


def test_strict_prompt_keeps_current_content_and_neighbor_evidence_separate():
    config = _minimal_config(retain_extraction_mode="custom", retain_custom_instructions=MARKER)
    current = "That option remains deferred."
    neighbor = "Option Nimbus: authorize three parcel comparisons."
    parts = build_chunk_prompt_parts(config, chunk=current, context=neighbor, metadata={"origin": "document"})
    request = json.loads(parts.user_message)
    assert request["current_content"] == current
    assert request["supporting_source_context"] == neighbor
    assert request["metadata"] == {"origin": "document"}
    assert "only" in request["task"] and "untrusted" in request["output_rules"]


def test_unmarked_prompt_keeps_legacy_format():
    parts = build_chunk_prompt_parts(_minimal_config(), chunk="A useful current fact.", context="Original context")
    assert parts.user_message.startswith("Extract facts from the following chunk.")
    assert "Content:" in parts.user_message and "A useful current fact." in parts.user_message
