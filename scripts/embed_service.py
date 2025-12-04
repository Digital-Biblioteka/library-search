import os
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

try:
    from sentence_transformers import SentenceTransformer
except Exception as e:
    SentenceTransformer = None

app = FastAPI(title="Embedding Service", version="1.0.0")


class EmbedRequest(BaseModel):
    text: str


class Embedder:
    def __init__(self) -> None:
        model_name = os.getenv(
            "EMBED_MODEL",
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        )
        if SentenceTransformer is None:
            raise RuntimeError(
                "sentence-transformers not installed. pip install -r scripts/requirements.txt"
            )
        self.model_name = model_name
        self.model = SentenceTransformer(model_name)

    def embed(self, text: str) -> Dict[str, Any]:
        vec = self.model.encode([text])[0].tolist()
        return {
            "vector": vec,
            "model": self.model_name,
            "dims": len(vec),
        }


_embedder: Embedder | None = None


def _get_embedder() -> Embedder:
    global _embedder
    if _embedder is None:
        _embedder = Embedder()
    return _embedder


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/healthz")
async def healthz() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/embed")
async def embed(req: EmbedRequest) -> Dict[str, Any]:
    text = (req.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="text must be non-empty")
    emb = _get_embedder().embed(text)
    return emb
