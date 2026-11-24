# Grounded RAG Agent

A command-line retrieval-augmented generation example. It chunks Markdown, reStructuredText, and plain-text files, stores embeddings in PostgreSQL with `pgvector`, retrieves the nearest passages, and uses the OpenAI Agents SDK to answer with source citations.

Retrieval runs before generation for every question. The agent receives only the threshold-filtered evidence, is instructed to ignore instructions inside source documents, and should say when the evidence is insufficient. Similarity thresholds do not guarantee answer quality; evaluate them against your corpus before production use.

## Requirements

- Python 3.11 or newer
- Docker Desktop, or PostgreSQL 16 with the `vector` extension installed
- An OpenAI API key with access to the configured answer and embedding models

## Setup on Windows PowerShell

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
Copy-Item .env.example .env
```

Set `OPENAI_API_KEY` in `.env`, then start the local database:

```powershell
docker compose up -d
python rag_agent.py init
```

The Compose password is for local development only. Change it before exposing or deploying the database, and keep `.env` out of version control.

## Index and ask

Put source files under a directory and run:

```powershell
python rag_agent.py ingest .\knowledge
python rag_agent.py ask "What does the source material say about ...?"
```

Re-ingesting a source replaces its previous chunks. Source paths and chunk indexes are included in citations. The program sends document chunks and user questions to OpenAI for embedding and generation; do not index data you are not authorized to share with that service.

## Configuration

`DATABASE_URL`, `OPENAI_API_KEY`, and `OPENAI_MODEL` are read from the environment or `.env`. `OPENAI_EMBEDDING_MODEL` defaults to `text-embedding-3-small` and must produce 1536 dimensions. `RAG_TOP_K` defaults to 5; `RAG_MIN_SIMILARITY` defaults to 0.25 and can be tuned for the corpus.

For deployment, use managed PostgreSQL with TLS, a least-privilege database role, secret management, network restrictions, monitoring, and tested backups. The Compose credentials and unauthenticated CLI are intended for local development, not as a multi-user service.

## Tests

```powershell
python -m pytest
```