import os
import json
import logging
import tempfile
import time
from pathlib import Path
from typing import List, Dict, Any

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from minio import Minio
from ingest.epub_to_json import read_epub

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
EMBED_URL = os.getenv("EMBED_URL", "http://localhost:8000/embed")
K_RRF = int(os.getenv("K_RRF", "60"))
BM25_SIZE = int(os.getenv("BM25_SIZE", "100"))
KNN_K = int(os.getenv("KNN_K", "100"))
KNN_NUM_CANDIDATES = int(os.getenv("KNN_NUM_CANDIDATES", "1000"))
TOP_N = int(os.getenv("TOP_N", "20"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("search_service")

ROOT = Path(__file__).resolve().parents[1]
MAP_DIR = ROOT / "mappings"

app = FastAPI(title="Library Search Service", version="1.0")


class SearchRequest(BaseModel):
    query: str | None = None
    title: str | None = None
    author: str | None = None
    genre: str | None = None
    description: str | None = None


class BookDoc(BaseModel):
    book_id: str | None = None
    title: str | None = None
    author: str | None = None
    publisher: str | None = None
    description: str | None = None
    genres: str | None = None
    linkToBook: str | None = None
    source_uid: str | None = None
    isbn: str | None = None


class IndexBookRequest(BaseModel):
    book_id: int
    title: str | None = None
    author: str | None = None
    publisher: str | None = None
    description: str | None = None
    genres: str | None = None
    linkToBook: str | None = None
    isbn: str | None = None


@app.get("/healthz")
def healthz() -> Dict[str, str]:
    return {"status": "ok"}


def _es_index_exists(name: str) -> bool:
    url = f"{ES_URL.rstrip('/')}/{name}"
    try:
        r = requests.get(url, timeout=10)
        return r.status_code == 200
    except Exception:
        return False


def _es_put(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{ES_URL.rstrip('/')}/{path.lstrip('/')}"
    resp = requests.put(url, json=body, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"ES PUT {path} failed: {resp.status_code} {resp.text}")
    if not resp.text:
        return {}
    return resp.json()


def _init_indices() -> None:
    indices = [
        ("books", MAP_DIR / "books.json"),
        ("book_content", MAP_DIR / "book_content.json"),
    ]

    deadline = time.time() + 120
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            r = requests.get(ES_URL, timeout=5)
            if r.status_code < 500:
                break
        except Exception as e:
            last_err = e
        time.sleep(2)
    else:
        raise RuntimeError(f"Elasticsearch not reachable at {ES_URL}: {last_err}")

    for name, mapping_path in indices:
        if _es_index_exists(name):
            continue
        body = json.loads(mapping_path.read_text(encoding="utf-8"))
        _es_put(name, body)
        log.info("Created Elasticsearch index: %s", name)


@app.on_event("startup")
def _on_startup() -> None:
    _init_indices()


def es_post(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{ES_URL.rstrip('/')}/{path.lstrip('/')}"
    resp = requests.post(url, json=body, timeout=30)
    if resp.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"ES POST {path} failed: {resp.status_code} {resp.text}")
    return resp.json()


def embed(text: str) -> List[float]:
    try:
        r = requests.post(EMBED_URL, json={"text": text}, timeout=60)
        r.raise_for_status()
        data = r.json()
        vec = data.get("vector")
        if not isinstance(vec, list):
            raise ValueError("embed returned no 'vector'")
        return [float(x) for x in vec]
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Embed failed: {e}")


def to_book_docs(es_body: Dict[str, Any]) -> List[BookDoc]:
    hits = (es_body or {}).get("hits", {})
    items = hits.get("hits", []) or []
    out: List[BookDoc] = []
    for h in items:
        src = h.get("_source") or {}
        score = h.get("_score")
        out.append(BookDoc(
            book_id=src.get("book_id"),
            title=src.get("title"),
            author=src.get("author"),
            publisher=src.get("publisher"),
            description=src.get("description"),
            genres=src.get("genres"),
            linkToBook=src.get("linkToBook"),
            source_uid=src.get("source_uid"),
            isbn=src.get("isbn"),
            score=float(score) if score is not None else None
        ))
    return out


def bm25_top_n(req: SearchRequest, size: int) -> List[BookDoc]:
    fields: List[str] = []
    if req.title:
        fields.append("title^4")
    if req.author:
        fields.append("author^3")
    if req.genre:
        fields.append("genres^2")
    fields.append("description")

    text_parts: List[str] = []
    if req.title:
        text_parts.append(req.title)
    if req.author:
        text_parts.append(req.author)
    if req.genre:
        text_parts.append(req.genre)
    if req.description:
        text_parts.append(req.description)
    if req.query:
        text_parts.append(req.query)

    query_text = " ".join(text_parts).strip()

    body = {
        "from": 0,
        "size": size,
        "query": {
            "multi_match": {
                "query": query_text,
                "fields": fields,
            }
        },
    }
    resp = es_post("books/_search", body)
    return to_book_docs(resp)


def knn_top_n(req: SearchRequest, k: int, num_candidates: int) -> List[BookDoc]:
    parts: List[str] = []
    if req.title:
        parts.append(f"Title: {req.title}")
    if req.author:
        parts.append(f"Author: {req.author}")
    if req.genre:
        parts.append(f"Genres: {req.genre}")
    if req.description:
        parts.append(f"Description: {req.description}")
    if req.query:
        parts.append(f"Query: {req.query}")

    text = "; ".join(parts).strip() or (req.query or "")

    vec = embed(text)
    body = {
        "knn": {
            "field": "description_vector",
            "query_vector": vec,
            "k": k,
            "num_candidates": max(100, num_candidates)
        }
    }
    resp = es_post("books/_search", body)
    return to_book_docs(resp)


def rrf_fuse(bm25: List[BookDoc], knn: List[BookDoc], k_rrf: int, top_n: int) -> List[BookDoc]:
    r_bm25: Dict[str, int] = {}
    r_knn: Dict[str, int] = {}
    by_id: Dict[str, BookDoc] = {}
    for i, d in enumerate(bm25):
        if d.book_id:
            r_bm25.setdefault(d.book_id, i + 1)
            by_id.setdefault(d.book_id, d)
    for i, d in enumerate(knn):
        if d.book_id:
            r_knn.setdefault(d.book_id, i + 1)
            by_id.setdefault(d.book_id, d)
    scores: Dict[str, float] = {}
    for bid in by_id.keys():
        rb = r_bm25.get(bid, 10**9)
        rk = r_knn.get(bid, 10**9)
        sb = 1.0 / (k_rrf + rb)
        sk = 1.0 / (k_rrf + rk)
        scores[bid] = sb + sk
    ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    out: List[BookDoc] = []
    for bid, _ in ordered[:top_n]:
        out.append(by_id[bid])
    if not out:
        out = bm25[:top_n]
    return out


def _minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("INGEST_USER") or "minioadmin"
    secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("INGEST_PASSWORD") or "minioadmin"
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


@app.post("/index/book")
def index_book(req: IndexBookRequest) -> Dict[str, Any]:
    """Индексировать книгу по её EPUB в MinIO.

    1. Скачиваем EPUB из RAW_BUCKET по linkToBook (или s3://bucket/key).
    2. Разбираем через read_epub -> получаем главы и метаданные.
    3. Формируем документ для индекса books и вектор описания.
    """

    raw_bucket = os.getenv("RAW_BUCKET", "raw")
    link = req.linkToBook or ""

    bucket = raw_bucket
    object_name = link
    if link.startswith("s3://"):
        # формат s3://bucket/key
        rest = link[len("s3://"):]
        parts = rest.split("/", 1)
        if len(parts) == 2:
            bucket, object_name = parts[0], parts[1]

    if not object_name:
        raise HTTPException(status_code=400, detail="linkToBook is required for indexing")

    client = _minio_client()

    # Скачиваем EPUB во временный файл и парсим его
    with tempfile.TemporaryDirectory() as td:
        tmp_path = Path(td) / Path(object_name).name
        resp = client.get_object(bucket, object_name)
        try:
            with tmp_path.open("wb") as w:
                for chunk in resp.stream(32 * 1024):
                    w.write(chunk)
        finally:
            resp.close()
            resp.release_conn()

        data = read_epub(tmp_path)

    # Переписываем/дополняем метаданные данными из Java
    data["book_id"] = str(req.book_id)
    if req.title:
        data["title"] = req.title
    if req.author:
        data["author"] = req.author
    if req.publisher:
        data["publisher"] = req.publisher
    if req.description:
        data["description"] = req.description
    if req.genres:
        data["genres"] = req.genres
    if req.isbn:
        data["isbn"] = req.isbn

    data["linkToBook"] = f"s3://{bucket}/{object_name}"

    # Текст для эмбеддинга: описание + первые параграфы
    text_parts: List[str] = []
    if "title" in data and data["title"]:
        text_parts.append(str(data["title"]))
    if "author" in data and data["author"]:
        text_parts.append(str(data["author"]))
    if "genres" in data and data["genres"]:
        text_parts.append(str(data["genres"]))
    if "description" in data and data["description"]:
        text_parts.append(str(data["description"]))

    # Добавим немного содержимого книги, если есть главы
    chapters = data.get("chapters") or []
    sample_paragraphs: List[str] = []
    for ch in chapters[:5]:
        paras = ch.get("paragraphs") or []
        sample_paragraphs.extend(paras[:20])
    if sample_paragraphs:
        text_parts.append("\n".join(sample_paragraphs))

    text = "\n\n".join([t for t in text_parts if t]).strip()

    vec: List[float] = []
    if text:
        vec = embed(text)

    doc: Dict[str, Any] = {
        "book_id": data.get("book_id"),
        "title": data.get("title", ""),
        "author": data.get("author", ""),
        "publisher": data.get("publisher", ""),
        "description": data.get("description", ""),
        "genres": data.get("genres", ""),
        "linkToBook": data.get("linkToBook", ""),
        "source_uid": data.get("source_uid"),
        "isbn": data.get("isbn", ""),
    }

    if vec:
        doc["description_vector"] = vec

    path = f"books/_doc/{data['book_id']}"
    es_post(path, doc)
    return {"status": "ok"}


@app.delete("/index/book/{book_id}")
def delete_book(book_id: int) -> Dict[str, Any]:
    url = f"{ES_URL.rstrip('/')}/books/_doc/{book_id}"
    try:
        r = requests.delete(url, timeout=10)
        if r.status_code == 404:
            return {"status": "not_found"}
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"ES DELETE failed: {r.status_code} {r.text}")
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"ES DELETE failed: {e}")


@app.post("/search/books", response_model=List[BookDoc])
def search_books(req: SearchRequest) -> List[BookDoc]:
    has_any = any([
        req.query,
        req.title,
        req.author,
        req.genre,
        req.description,
    ])
    if not has_any:
        return []
    bm25 = bm25_top_n(req, BM25_SIZE)
    try:
        knn = knn_top_n(req, KNN_K, KNN_NUM_CANDIDATES)
    except HTTPException:
        log.warning("Embed/knn failed; fallback to BM25 only")
        knn = []
    return rrf_fuse(bm25, knn, K_RRF, TOP_N)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8001"))
    uvicorn.run("scripts.search_service:app", host="0.0.0.0", port=port, reload=False)
