"""Guard: filing change detection tolerates every pgvector column
representation — via the owned nousergon_lib.rag chokepoint.

Regression (2026-07-11 weekly-freshness break): the RAG weekly ingestion
crashed at Step 8/9 (filing change detection) with

    File ".../rag/pipelines/filing_change_detection.py",
        in _load_filing_embeddings
      vec = np.array(embedding, dtype=np.float32)
    TypeError: float() argument must be a string or a real number, not 'Vector'

``nousergon_lib.rag.get_connection`` registers pgvector's psycopg2 codec to
return numpy arrays, but the pgvector/psycopg2 build resolved on the spot
handed back a ``pgvector.Vector`` object instead. ``pgvector.Vector`` has no
numpy interop, so ``np.array(v, dtype=np.float32)`` calls ``float(v)`` and
detonates.

The call-site fix (this repo's former local ``_embedding_to_f32``,
nousergon-data PR #747) was LIFTED to the owned chokepoint
``nousergon_lib.rag.coerce_embedding`` (config#2221) so ANY consumer that
reads a ``vector`` column normalizes identically and no future consumer doing
``np.array(...)`` can reintroduce the crash. This test now exercises the lib
helper as the pipeline consumes it, and asserts the pipeline no longer carries
its own coercer (which would silently drift from the chokepoint again).
"""

from __future__ import annotations

import numpy as np
import pytest

from nousergon_lib.rag import coerce_embedding


class _VectorLike:
    """Mimics ``pgvector.Vector``: exposes ``.to_numpy()`` and, crucially,
    NO ``__array__``/``__len__``/``__iter__`` — so a naive
    ``np.array(obj, dtype=np.float32)`` raises exactly as prod did."""

    def __init__(self, values):
        self._values = list(values)

    def to_numpy(self):
        return np.array(self._values, dtype=np.float32)


def test_naive_cast_reproduces_the_regression():
    # Lock the failure mode the fix defends against: a to_numpy-only object
    # is NOT coercible by np.array directly.
    with pytest.raises(TypeError):
        np.array(_VectorLike([1.0, 2.0, 3.0]), dtype=np.float32)


def test_vector_like_object_is_coerced():
    out = coerce_embedding(_VectorLike([1.0, 2.0, 3.0]))
    assert isinstance(out, np.ndarray)
    assert out.dtype == np.float32
    np.testing.assert_array_equal(out, np.array([1, 2, 3], dtype=np.float32))


def test_ndarray_passthrough():
    # The register_vector "happy path" — value already an ndarray (float64).
    out = coerce_embedding(np.array([1.5, 2.5], dtype=np.float64))
    assert out.dtype == np.float32
    np.testing.assert_array_equal(out, np.array([1.5, 2.5], dtype=np.float32))


def test_list_is_coerced():
    out = coerce_embedding([0.1, 0.2, 0.3])
    assert out.dtype == np.float32
    assert out.shape == (3,)


def test_raw_string_fails_loud():
    # A raw string means the codec silently didn't register — must surface,
    # never be silently re-parsed.
    with pytest.raises((ValueError, TypeError)):
        coerce_embedding("[1,2,3]")


def test_real_pgvector_vector_if_available():
    # When pgvector is installed (it is in CI via requirements.txt →
    # nousergon-lib[rag]), lock the exact prod type, not just the stub.
    pgvector = pytest.importorskip("pgvector")
    Vector = pgvector.Vector
    out = coerce_embedding(Vector([4.0, 5.0, 6.0]))
    assert out.dtype == np.float32
    np.testing.assert_array_equal(out, np.array([4, 5, 6], dtype=np.float32))


def test_pipeline_consumes_the_lib_chokepoint_not_a_local_coercer():
    """The migration's whole point: the pipeline must delegate to the lib
    chokepoint and NOT reintroduce a local ``_embedding_to_f32`` that could
    drift from it. Asserts the symbol is gone."""
    import rag.pipelines.filing_change_detection as fcd

    assert not hasattr(fcd, "_embedding_to_f32"), (
        "the local _embedding_to_f32 was lifted to "
        "nousergon_lib.rag.coerce_embedding (config#2221) — do not reintroduce "
        "a local coercer"
    )
