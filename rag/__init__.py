"""RAG namespace package — ingestion side.

The shared retrieval / db / embeddings / schema code now lives in
``nousergon_lib.rag`` (since lib v0.3.0). This folder retains only the
weekly ingestion ``pipelines/`` subpackage and ``preflight.py``.

The lib's own ``__init__`` auto-loads ``.env`` for ``RAG_DATABASE_URL`` and
``VOYAGE_API_KEY``, so no duplication is needed here.
"""
