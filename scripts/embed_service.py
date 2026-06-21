import os
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

try:
    from sentence_transformers import SentenceTransformer
except Exception as e:
    SentenceTransformer = None

app = FastAPI(title="Embedding Service", version="1.0.0")


class EmbedRequest(BaseModel):
    text: str


class RerankRequest(BaseModel):
    query: str
    texts: List[str]


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


class Reranker:
    def __init__(self) -> None:
        from transformers import AutoTokenizer, AutoModelForSequenceClassification

        model_name = os.getenv(
            "CROSS_ENCODER_MODEL",
            "BAAI/bge-reranker-v2-m3",
        )
        self.model_name = model_name
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self._is_bge = "bge" in model_name.lower()

    def rerank(self, query: str, texts: List[str]) -> List[float]:
        if self._is_bge:
            pairs = [[f"query: {query}", f"passage: {text}"] for text in texts]
        else:
            pairs = [[query, text] for text in texts]
        inputs = self.tokenizer(
            pairs,
            padding=True,
            truncation=True,
            return_tensors="pt",
            max_length=512,
        )
        import torch

        with torch.no_grad():
            outputs = self.model(**inputs)
        scores = torch.sigmoid(outputs.logits).squeeze(-1).tolist()
        if isinstance(scores, float):
            scores = [scores]
        return scores


_embedder: Embedder | None = None
_reranker: Reranker | None = None


def _get_embedder() -> Embedder:
    global _embedder
    if _embedder is None:
        _embedder = Embedder()
    return _embedder


def _get_reranker() -> Reranker:
    global _reranker
    if _reranker is None:
        _reranker = Reranker()
    return _reranker


@app.on_event("startup")
def _on_startup() -> None:
    _get_embedder()
    _get_reranker()


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/healthz")
async def healthz() -> Dict[str, str]:
    if _embedder is None or _reranker is None:
        raise HTTPException(status_code=503, detail="models not loaded yet")
    return {"status": "ok"}


@app.post("/embed")
async def embed(req: EmbedRequest) -> Dict[str, Any]:
    text = (req.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="text must be non-empty")
    emb = _get_embedder().embed(text)
    return emb


@app.post("/rerank")
async def rerank(req: RerankRequest) -> Dict[str, Any]:
    query = (req.query or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query must be non-empty")
    if not req.texts:
        raise HTTPException(status_code=400, detail="texts must be non-empty")
    scores = _get_reranker().rerank(query, req.texts)
    indexed = list(enumerate(scores))
    indexed.sort(key=lambda x: x[1], reverse=True)
    return {
        "scores": scores,
        "ranks": [{"index": i, "score": s} for i, s in indexed],
    }