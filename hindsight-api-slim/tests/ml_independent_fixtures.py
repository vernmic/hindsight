"""Deterministic ML-independent fixtures for row/tag behaviour tests.

These override the session embeddings and cross-encoder fixtures with the native
torch-free stubs so real-persistence tag assertions run without the optional
local-ml extra. Embedding quality is irrelevant to these tests; the real pg0
persistence owner and every asserted state transition are unchanged.
"""

import asyncio

import pytest

from tests.test_recall_min_score import _StubReranker
from tests.test_retain_same_document_concurrency import _StubEmbeddings


@pytest.fixture(scope="session")
def embeddings():
    emb = _StubEmbeddings()
    asyncio.run(emb.initialize())
    return emb


@pytest.fixture(scope="session")
def cross_encoder():
    ce = _StubReranker("stub")
    asyncio.run(ce.initialize())
    return ce
