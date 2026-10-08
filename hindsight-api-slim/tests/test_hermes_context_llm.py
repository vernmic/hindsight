"""Real extractor acceptance; run in the repository's hs_llm_core provider lane."""

import pytest
from hindsight_api.config import HindsightConfig
from hindsight_api.engine.retain.fact_extraction import extract_facts_from_text
from hindsight_api.engine.retain.source_context import MARKER


@pytest.mark.hs_llm_core
@pytest.mark.asyncio
async def test_acknowledgement_cannot_retain_neighbor_only_knowledge(llm_config):
    config = HindsightConfig.from_env()
    config.retain_extraction_mode = "custom"
    config.retain_custom_instructions = (
        "Extract only useful source-supported assertions made in current content. "
        "Neighboring context resolves current references but is not an extraction target. "
        "Acknowledgements and disposable completion/report notices yield no facts. " + MARKER
    )
    facts, chunks, usage = await extract_facts_from_text(
        "Thanks. I wrote the report to /tmp/result.txt.",
        None,
        llm_config,
        config,
        context="The Nimbus Analysis skill supports parcel quote comparisons.",
    )
    assert not facts, "Only source context contains useful knowledge; current content adds no useful assertion"
