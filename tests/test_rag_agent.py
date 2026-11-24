import pytest

from rag_agent import Settings, chunk_text, relative_source


def test_chunk_text_returns_empty_for_whitespace() -> None:
    assert chunk_text(" \n\t ") == []


def test_chunk_text_preserves_content_with_overlap() -> None:
    text = " ".join(f"word{index}" for index in range(100))

    chunks = chunk_text(text, chunk_tokens=30, overlap=5)

    assert len(chunks) > 1
    assert all(chunk.strip() for chunk in chunks)
    assert chunks[0] in text
    assert chunks[-1].split()[-1] == "word99"


@pytest.mark.parametrize(
    ("chunk_tokens", "overlap"),
    [(0, 0), (10, -1), (10, 10)],
)
def test_chunk_text_rejects_invalid_sizes(chunk_tokens: int, overlap: int) -> None:
    with pytest.raises(ValueError):
        chunk_text("text", chunk_tokens=chunk_tokens, overlap=overlap)


def test_relative_source_uses_portable_paths(tmp_path) -> None:
    root = tmp_path / "docs"
    source = root / "nested" / "policy.md"

    assert relative_source(source, root) == "nested/policy.md"


def test_settings_reject_invalid_retrieval_values(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/rag")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("RAG_TOP_K", "21")

    with pytest.raises(ValueError, match="RAG_TOP_K"):
        Settings.from_env()