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


class ContentSearchRequest(BaseModel):
    query: str
    book_id: str | None = None
    size: int = 10


class ContentSearchResult(BaseModel):
    book_id: str
    title: str = ""
    author: str = ""
    chapter: str = ""
    chapter_index: int = 0
    spine_index: int = -1
    paragraph_index: int = 0
    text_snippet: str = ""
    score: float = 0.0


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


def es_bulk(actions: list[tuple]) -> Dict[str, Any]:
    if not actions:
        return {}
    lines: List[str] = []
    for meta, src in actions:
        lines.append(json.dumps(meta, ensure_ascii=False))
        lines.append(json.dumps(src, ensure_ascii=False))
    body = "\n".join(lines) + "\n"
    url = f"{ES_URL.rstrip('/')}/_bulk"
    resp = requests.post(
        url,
        data=body.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        timeout=120,
    )
    if resp.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"ES bulk failed: {resp.status_code} {resp.text[:500]}")
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

    suggest_input: List[str] = []
    if data.get("title"):
        suggest_input.append(str(data["title"]))
    if data.get("author"):
        suggest_input.append(str(data["author"]))
    if suggest_input:
        doc["suggest"] = {"input": suggest_input}

    path = f"books/_doc/{data['book_id']}"
    es_post(path, doc)

    chapters = data.get("chapters") or []
    if chapters:
        bulk_actions: list[tuple] = []
        seq = 0
        for chapter_index, ch in enumerate(chapters):
            chapter_name = ch.get("chapter", "")
            paragraphs = ch.get("paragraphs") or []
            for paragraph_index, para in enumerate(paragraphs):
                if not para or not para.strip():
                    continue
                content_doc: Dict[str, Any] = {
                    "book_id": str(req.book_id),
                    "chunk_id": f"{req.book_id}-{seq:06d}",
                    "chapter": chapter_name,
                    "chapter_index": chapter_index,
                    "spine_index": ch.get("spine_index", -1),
                    "paragraph_index": paragraph_index,
                    "text": para,
                }
                try:
                    para_vec = embed(para[:1000])
                    content_doc["text_vector"] = para_vec
                except Exception:
                    log.warning("Failed to embed paragraph %d of book %s", seq, req.book_id)
                bulk_actions.append(({"index": {"_index": "book_content"}}, content_doc))
                seq += 1
        if bulk_actions:
            es_bulk(bulk_actions)
            log.info("Indexed %d paragraphs into book_content for book %s", seq, req.book_id)

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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"ES DELETE failed: {e}")

    content_url = f"{ES_URL.rstrip('/')}/book_content/_delete_by_query"
    content_body = {"query": {"term": {"book_id": str(book_id)}}}
    try:
        r = requests.post(content_url, json=content_body, timeout=30)
        if r.status_code >= 400:
            log.warning("Failed to delete book_content for book %s: %s", book_id, r.text)
    except Exception as e:
        log.warning("Failed to delete book_content for book %s: %s", book_id, e)

    return {"status": "ok"}


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


def _content_bm25(query: str, book_id: str | None, size: int) -> List[Dict[str, Any]]:
    must: List[Dict[str, Any]] = [{"match": {"text": {"query": query}}}]
    if book_id:
        must.append({"term": {"book_id": book_id}})
    body: Dict[str, Any] = {
        "size": size,
        "query": {"bool": {"must": must}},
        "highlight": {
            "fields": {
                "text": {
                    "fragment_size": 150,
                    "number_of_fragments": 1,
                }
            }
        },
    }
    resp = es_post("book_content/_search", body)
    hits = resp.get("hits", {}).get("hits", [])
    results: List[Dict[str, Any]] = []
    for h in hits:
        src = h.get("_source", {})
        highlight = h.get("highlight", {})
        text_snippet = (highlight.get("text") or [src.get("text", "")[:150]])[0]
        results.append({
            "book_id": src.get("book_id", ""),
            "chapter": src.get("chapter", ""),
            "chapter_index": src.get("chapter_index", 0),
            "spine_index": src.get("spine_index", -1),
            "paragraph_index": src.get("paragraph_index", 0),
            "text_snippet": text_snippet,
            "score": float(h.get("_score", 0)),
        })
    return results


def _content_knn(query: str, book_id: str | None, k: int, num_candidates: int) -> List[Dict[str, Any]]:
    vec = embed(query)
    knn_clause: Dict[str, Any] = {
        "field": "text_vector",
        "query_vector": vec,
        "k": k,
        "num_candidates": max(100, num_candidates),
    }
    if book_id:
        knn_clause["filter"] = [{"term": {"book_id": book_id}}]
    body = {"size": k, "knn": knn_clause}
    resp = es_post("book_content/_search", body)
    hits = resp.get("hits", {}).get("hits", [])
    results: List[Dict[str, Any]] = []
    for h in hits:
        src = h.get("_source", {})
        results.append({
            "book_id": src.get("book_id", ""),
            "chapter": src.get("chapter", ""),
            "chapter_index": src.get("chapter_index", 0),
            "spine_index": src.get("spine_index", -1),
            "paragraph_index": src.get("paragraph_index", 0),
            "text_snippet": src.get("text", "")[:150],
            "score": float(h.get("_score", 0)),
        })
    return results


def _content_rrf(bm25: List[Dict], knn: List[Dict], k_rrf: int, top_n: int) -> List[Dict]:
    by_key: Dict[str, Dict] = {}
    r_bm25: Dict[str, int] = {}
    r_knn: Dict[str, int] = {}
    for i, r in enumerate(bm25):
        key = f"{r['book_id']}_{r['chapter_index']}_{r['paragraph_index']}"
        r_bm25.setdefault(key, i + 1)
        by_key.setdefault(key, r)
    for i, r in enumerate(knn):
        key = f"{r['book_id']}_{r['chapter_index']}_{r['paragraph_index']}"
        r_knn.setdefault(key, i + 1)
        by_key.setdefault(key, r)
    scores: Dict[str, float] = {}
    for key in by_key:
        rb = r_bm25.get(key, 10**9)
        rk = r_knn.get(key, 10**9)
        scores[key] = 1.0 / (k_rrf + rb) + 1.0 / (k_rrf + rk)
    ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    return [by_key[k] for k, _ in ordered[:top_n]]


def _fetch_book_metadata(book_ids: List[str]) -> Dict[str, Dict[str, str]]:
    if not book_ids:
        return {}
    url = f"{ES_URL.rstrip('/')}/books/_mget"
    body = {"ids": list(set(book_ids))}
    try:
        resp = requests.get(url, json=body, timeout=10)
        if resp.status_code >= 400:
            return {}
        docs = resp.json().get("docs", [])
        result: Dict[str, Dict[str, str]] = {}
        for doc in docs:
            if doc.get("found"):
                src = doc.get("_source", {})
                bid = src.get("book_id") or doc.get("id")
                result[str(bid)] = {
                    "title": src.get("title", ""),
                    "author": src.get("author", ""),
                }
        return result
    except Exception:
        return {}


@app.post("/search/content", response_model=List[ContentSearchResult])
def search_content(req: ContentSearchRequest) -> List[ContentSearchResult]:
    if not req.query.strip():
        return []
    bm25 = _content_bm25(req.query, req.book_id, BM25_SIZE)
    try:
        knn = _content_knn(req.query, req.book_id, KNN_K, KNN_NUM_CANDIDATES)
    except HTTPException:
        log.warning("Embed/knn failed for content search; fallback to BM25 only")
        knn = []
    fused = _content_rrf(bm25, knn, K_RRF, req.size)
    book_ids = [r["book_id"] for r in fused if r.get("book_id")]
    meta = _fetch_book_metadata(book_ids)
    results: List[ContentSearchResult] = []
    for r in fused:
        bid = r.get("book_id", "")
        book_info = meta.get(bid, {})
        results.append(ContentSearchResult(
            book_id=bid,
            title=book_info.get("title", ""),
            author=book_info.get("author", ""),
            chapter=r.get("chapter", ""),
            chapter_index=r.get("chapter_index", 0),
            spine_index=r.get("spine_index", -1),
            paragraph_index=r.get("paragraph_index", 0),
            text_snippet=r.get("text_snippet", ""),
            score=r.get("score", 0.0),
        ))
    return results


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8001"))
    uvicorn.run("scripts.search_service:app", host="0.0.0.0", port=port, reload=False)
