import os
import json
import logging
import re
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
RERANK_URL = os.getenv("RERANK_URL", "http://localhost:8000/rerank")
K_RRF = int(os.getenv("K_RRF", "60"))
BM25_SIZE = int(os.getenv("BM25_SIZE", "200"))
KNN_K = int(os.getenv("KNN_K", "100"))
KNN_NUM_CANDIDATES = int(os.getenv("KNN_NUM_CANDIDATES", "1000"))
TOP_N = int(os.getenv("TOP_N", "20"))
RERANK_TOP_K = int(os.getenv("RERANK_TOP_K", "20"))
RERANK_MIN_SCORE_DELTA = float(os.getenv("RERANK_MIN_SCORE_DELTA", "0.001"))
ASK_RELEVANCE_THRESHOLD = float(os.getenv("ASK_RELEVANCE_THRESHOLD", "1.0"))

EMBED_MODE = os.getenv("EMBED_MODE", "local")
EMBED_API_URL = os.getenv("EMBED_API_URL", "").rstrip("/")
EMBED_API_KEY = os.getenv("EMBED_API_KEY", "")
EMBED_API_MODEL = os.getenv("EMBED_API_MODEL", "text-embedding-ada-002")
BOOKS_INDEX = os.getenv("BOOKS_INDEX", "books_api" if EMBED_MODE == "api" else "books")

LLM_API_URL = os.getenv("LLM_API_URL", EMBED_API_URL).rstrip("/")
LLM_API_KEY = os.getenv("LLM_API_KEY", EMBED_API_KEY)
LLM_MODEL = os.getenv("LLM_MODEL", "deepseek/deepseek-v4-flash")
LLM_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "512"))

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("search_service")

ROOT = Path(__file__).resolve().parents[1]
MAP_DIR = ROOT / "mappings"

app = FastAPI(title="Library Search Service", version="1.0")

SYNONYM_MAP: Dict[str, str] = {
    "crime": "преступление", "преступление": "crime",
    "punishment": "наказание", "наказание": "punishment",
    "war": "война", "война": "war",
    "peace": "мир", "мир": "peace",
    "anna": "анна", "анна": "anna",
    "karenina": "каренина", "каренина": "karenina",
    "brothers": "братья", "братья": "brothers",
    "karamazov": "карамазовы", "карамазовы": "karamazov",
    "idiot": "идиот", "идиот": "idiot",
    "fathers": "отцы", "отцы": "fathers",
    "children": "дети", "дети": "children",
    "love": "любовь", "любовь": "love",
    "death": "смерть", "смерть": "death",
    "soul": "душа", "душа": "soul",
    "dead": "мертвый", "мертвый": "dead",
    "souls": "души", "души": "souls",
    "captain": "капитан", "капитан": "captain",
    "daughter": "дочь", "дочь": "daughter",
    "garden": "сад", "сад": "garden",
    "robinson": "робинзон", "робинзон": "robinson",
    "dracula": "дракула", "дракула": "dracula",
    "alice": "алиса", "алиса": "alice",
    "childhood": "детство", "детство": "childhood",
    "adolescence": "отрочество", "отрочество": "adolescence",
    "youth": "юность", "юность": "youth",
    "demons": "бесы", "бесы": "demons",
    "gulliver": "гулливер", "гулливер": "gulliver",
    "tom": "том", "том": "tom",
    "sawyer": "сойер", "сойер": "sawyer",
    "huckleberry": "гекльберри", "гекльберри": "huckleberry",
    "fin": "финн", "финн": "fin",
    "onegin": "онегин", "онегин": "onegin",
    "evgeny": "евгений", "евгений": "evgeny",
    "taras": "тарас", "тарас": "taras",
    "bulba": "бульба", "бульба": "bulba",
    "mother": "мать", "мать": "mother",
    "father": "отец", "отец": "father",
    "prince": "князь", "князь": "prince",
    "myshkin": "мышкин", "мышкин": "myshkin",
    "dostoevsky": "dostoyevsky",
    "dostoyevsky": "достоевский", "достоевский": "dostoyevsky",
    "tolstoy": "толстой", "толстой": "tolstoy",
    "turgenev": "тургенев", "тургенев": "turgenev",
    "gogol": "гоголь", "гоголь": "gogol",
    "chekhov": "чехов", "чехов": "chekhov",
    "pushkin": "пушкин", "пушкин": "pushkin",
    "lermontov": "лермонтов", "лермонтов": "lermontov",
    "goncharov": "гончаров", "гончаров": "goncharov",
    "shakespeare": "шекспир", "шекспир": "shakespeare",
    "dickens": "диккенс", "диккенс": "dickens",
    "twain": "твен", "твен": "twain",
    "wilde": "уайльд", "уайльд": "wilde",
    "kafka": "кафка", "кафка": "kafka",
    "frankenstein": "франкенштейн", "франкенштейн": "frankenstein",
    "orwell": "оруэлл", "оруэлл": "orwell",
    "huxley": "хаксли", "хаксли": "huxley",
    "defoe": "дефо", "дефо": "defoe",
    "baum": "баум", "баум": "baum",
    "austen": "остин", "остин": "austen",
    "dostoevskii": "dostoevsky",
    "dostoyevskiy": "dostoyevsky",
    "dostojevskij": "dostoyevsky",
    "tolstoi": "tolstoy",
    "tolstoj": "tolstoy",
    "turgeneff": "turgenev",
    "gogolj": "gogol",
    "chekov": "chekhov",
    "lermontoff": "lermontov",
    "goncharoff": "goncharov",
    "shekspir": "shakespeare",
    "dikkens": "dickens",
    "uayld": "wilde",
    "servantes": "cervantes",
    "cervantes": "сервантес", "сервантес": "cervantes",
    "byron": "байрон", "байрон": "byron",
    "wells": "уэллс", "уэллс": "wells",
    "verne": "верн", "верн": "verne",
    "jane": "джейн", "джейн": "jane",
    "ostin": "austen",
}


def expand_query(text: str) -> str:
    words = text.split()
    expanded = []
    for w in words:
        cleaned = w.strip(".,!?;:\"'()[]{}")
        punct_before = w[:len(w) - len(cleaned)]
        punct_after = w[len(cleaned):]
        synonym = SYNONYM_MAP.get(cleaned.lower())
        if synonym and synonym.lower() != cleaned.lower():
            expanded.append(f"{punct_before}{cleaned} {synonym}{punct_after}")
        else:
            expanded.append(w)
    return " ".join(expanded)


def _fuzzy_synonym_match(text: str) -> str:
    import difflib
    words = text.split()
    synonyms: List[str] = []
    for w in words:
        cleaned = w.strip(".,!?;:\"'()[]{}").lower()
        if not cleaned:
            continue
        matches = difflib.get_close_matches(cleaned, SYNONYM_MAP.keys(), n=1, cutoff=0.7)
        if matches:
            synonym = SYNONYM_MAP[matches[0]]
            if synonym.lower() != cleaned.lower():
                synonyms.append(synonym)
    return " ".join(synonyms)


def _extract_synonyms_only(text: str) -> str:
    words = text.split()
    synonyms: List[str] = []
    for w in words:
        cleaned = w.strip(".,!?;:\"'()[]{}")
        if not cleaned:
            continue
        synonym = SYNONYM_MAP.get(cleaned.lower())
        if synonym and synonym.lower() != cleaned.lower():
            synonyms.append(synonym)
    result = " ".join(synonyms)
    if not result:
        result = _fuzzy_synonym_match(text)
    return result


def _detect_lang(text: str) -> str:
    for ch in text:
        if 'а' <= ch.lower() <= 'я':
            return "russian"
    return "english"


def _get_no_relevant_msg(lang: str) -> str:
    if lang == "english":
        return "Sorry, no relevant passages were found for your question in this book."
    return "Извините, по вашему вопросу не найдено релевантных отрывков в книге."


def _get_not_about_book_msg(lang: str) -> str:
    if lang == "english":
        return "This question does not appear to be related to this book. Please ask a question about the book's content."
    return "Этот вопрос не относится к данной книге. Пожалуйста, задайте вопрос по содержанию книги."


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
    language: str | None = None
    score: float | None = None
    rrf_score: float | None = None


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


class AskBookRequest(BaseModel):
    question: str
    book_id: str
    top_k: int = 10


class AskBookResponse(BaseModel):
    answer: str
    sources: list[dict[str, Any]] = []


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
        ("books_api", MAP_DIR / "books.json"),
        ("book_content", MAP_DIR / "book_content.json"),
        ("reviews", MAP_DIR / "reviews.json"),
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
    if not path.startswith("http"):
        url = f"{ES_URL.rstrip('/')}/{path.lstrip('/')}"
    else:
        url = path
    headers = {"Content-Type": "application/json"}
    resp = requests.post(url, json=body, headers=headers, timeout=30)
    if resp.status_code >= 400:
        log.warning("Elasticsearch POST %s failed: %s %s", path, resp.status_code, resp.text)
        raise HTTPException(status_code=502, detail=f"ES POST {path} failed: {resp.text}")
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


def embed_local(text: str) -> List[float]:
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


def embed_api(text: str) -> List[float] | None:
    if not EMBED_API_URL or not EMBED_API_KEY:
        log.info("API embedding not configured (EMBED_API_URL or EMBED_API_KEY missing)")
        return None
    try:
        headers = {
            "Authorization": f"Bearer {EMBED_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": EMBED_API_MODEL,
            "input": text,
        }
        log.info(f"Calling API embedding: {EMBED_API_URL}, model={EMBED_API_MODEL}")
        resp = requests.post(f"{EMBED_API_URL}/embeddings", json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        vec = data["data"][0]["embedding"]
        if not isinstance(vec, list):
            raise ValueError("API embed returned no 'embedding'")
        result = [float(x) for x in vec]
        log.info(f"API embedding succeeded, dims={len(result)}")
        return result
    except Exception as e:
        log.warning(f"API embedding failed ({e}); will fall back to local model")
        return None


def embed(text: str) -> List[float]:
    if EMBED_MODE == "api":
        result = embed_api(text)
        if result is not None:
            return result
        log.info("API embedding failed, falling back to local embed service")
    return embed_local(text)


def rerank(query: str, texts: List[str]) -> Dict[str, Any]:
    try:
        r = requests.post(RERANK_URL, json={"query": query, "texts": texts}, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"Re-rank failed ({e}); falling back to original order")
        return {"scores": [0.0] * len(texts), "ranks": [{"index": i, "score": 0.0} for i in range(len(texts))]}


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
            language=src.get("language"),
            score=float(score) if score is not None else None
        ))
    return out


def _token_count(text: str) -> int:
    return len(text.strip().split())


def _build_min_should_match(text: str) -> str:
    n = _token_count(text)
    if n <= 2:
        return "100%"
    return f"{n - 1}<{n - 1} {n}<75%"


def bm25_top_n(req: SearchRequest, size: int) -> List[BookDoc]:
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

    is_simple = bool(req.query and not req.title and not req.author and not req.genre and not req.description)

    if is_simple:
        should = [
            {"match_phrase": {"title": {"query": query_text, "boost": 100, "slop": 2}}},
            {"match_phrase": {"author": {"query": query_text, "boost": 40, "slop": 2}}},
        ]

        if req.query and len(req.query.strip().split()) <= 2:
            should.append({"match": {"title": {"query": req.query, "boost": 3, "fuzziness": 1, "prefix_length": 1}}})
            should.append({"match": {"author": {"query": req.query, "boost": 2, "fuzziness": 1, "prefix_length": 1}}})

        cross_lingual_words = _extract_synonyms_only(query_text)
        if cross_lingual_words:
            should.append({
                "match_phrase": {
                    "author": {
                        "query": cross_lingual_words,
                        "boost": 15,
                        "slop": 2,
                    }
                }
            })
            should.append({
                "multi_match": {
                    "query": cross_lingual_words,
                    "fields": ["title^20", "author^10"],
                    "type": "best_fields",
                }
            })
    else:
        fields: List[str] = []
        if req.title:
            fields.append("title^4")
        elif req.query:
            fields.append("title^5")
        if req.author:
            fields.append("author^3")
        elif req.query:
            fields.append("author^3")
        if req.genre:
            fields.append("genres^2")
        if req.description:
            fields.append("description")

        should = [
            {"match_phrase": {"title": {"query": query_text, "boost": 50, "slop": 2}}},
            {"multi_match": {"query": query_text, "fields": fields, "fuzziness": "AUTO"}},
        ]

    body = {
        "from": 0,
        "size": size,
        "query": {
            "bool": {
                "should": should,
                "minimum_should_match": 1,
            }
        },
    }
    resp = es_post(f"{BOOKS_INDEX}/_search", body)
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
    resp = es_post(f"{BOOKS_INDEX}/_search", body)
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
    for bid, score_val in ordered[:top_n]:
        d = by_id[bid]
        d.rrf_score = score_val
        out.append(d)
    if not out:
        out = bm25[:top_n]
    return out


def rerank_by_language(docs: List[BookDoc], query_lang: str, is_simple: bool = False) -> List[BookDoc]:
    match: List[BookDoc] = []
    other: List[BookDoc] = []

    for d in docs:
        if d.language is None or d.language == "":
            match.append(d)
        elif d.language.lower() == query_lang.lower():
            match.append(d)
        else:
            other.append(d)

    match.sort(key=lambda d: d.rrf_score or d.score or 0.0, reverse=True)
    other.sort(key=lambda d: d.rrf_score or d.score or 0.0, reverse=True)

    return match + other


def _minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("INGEST_USER") or "minioadmin"
    secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("INGEST_PASSWORD") or "minioadmin"
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


@app.post("/index/book")
def index_book(req: IndexBookRequest) -> Dict[str, Any]:
    raw_bucket = os.getenv("RAW_BUCKET", "raw")
    link = req.linkToBook or ""

    bucket = raw_bucket
    object_name = link
    if link.startswith("s3://"):
        rest = link[len("s3://"):]
        parts = rest.split("/", 1)
        if len(parts) == 2:
            bucket, object_name = parts[0], parts[1]

    if not object_name:
        raise HTTPException(status_code=400, detail="linkToBook is required for indexing")

    client = _minio_client()

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

    text_parts: List[str] = []
    if "title" in data and data["title"]:
        text_parts.append(str(data["title"]))
    if "author" in data and data["author"]:
        text_parts.append(str(data["author"]))
    if "genres" in data and data["genres"]:
        text_parts.append(str(data["genres"]))
    if "description" in data and data["description"]:
        text_parts.append(str(data["description"]))

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
        "language": _detect_lang(text) if text else None,
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

    path = f"{BOOKS_INDEX}/_doc/{data['book_id']}"
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
    url = f"{ES_URL.rstrip('/')}/{BOOKS_INDEX}/_doc/{book_id}"
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

    is_simple = (
        req.query
        and not req.title
        and not req.author
        and not req.genre
        and not req.description
    )

    expanded = SearchRequest(
        query=req.query,
        title=expand_query(req.title) if req.title else None,
        author=expand_query(req.author) if req.author else None,
        genre=expand_query(req.genre) if req.genre else None,
        description=expand_query(req.description) if req.description else None,
    )
    bm25 = bm25_top_n(expanded, BM25_SIZE)

    if is_simple:
        docs = bm25[:TOP_N]
        if len(bm25) < 3 and req.query:
            import difflib
            q = req.query.strip().lower()
            if difflib.get_close_matches(q, SYNONYM_MAP.keys(), n=1, cutoff=0.6):
                fallback_req = SearchRequest(
                    query=req.query,
                    title=None, author=None, genre=None, description=None,
                )
                docs = bm25_top_n(fallback_req, BM25_SIZE)[:TOP_N]
    else:
        try:
            knn = knn_top_n(expanded, KNN_K, KNN_NUM_CANDIDATES)
        except HTTPException:
            knn = []
        docs = rrf_fuse(bm25, knn, K_RRF, TOP_N)

    query_text = req.query or req.title or ""
    if query_text:
        query_lang = _detect_lang(query_text)
        docs = rerank_by_language(docs, query_lang, is_simple=is_simple)
    return docs



@app.post("/search/books/semantic", response_model=List[BookDoc])
def search_books_semantic(req: SearchRequest) -> List[BookDoc]:
    has_any = any([
        req.query,
        req.title,
        req.author,
        req.genre,
        req.description,
    ])
    if not has_any:
        return []
    try:
        knn = knn_top_n(req, KNN_K, KNN_NUM_CANDIDATES)
    except HTTPException:
        log.warning("Embed/knn failed; semantic search falling back to BM25")
        knn = []
    if not knn:
        expanded = SearchRequest(
            query=req.query,
            title=expand_query(req.title) if req.title else None,
            author=expand_query(req.author) if req.author else None,
            genre=expand_query(req.genre) if req.genre else None,
            description=expand_query(req.description) if req.description else None,
        )
        bm25 = bm25_top_n(expanded, TOP_N)
        docs = bm25
    else:
        docs = knn[:TOP_N]
    query_text = req.query or req.title or ""
    if query_text:
        query_lang = _detect_lang(query_text)
        docs = rerank_by_language(docs, query_lang)
    return docs


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
    url = f"{ES_URL.rstrip('/')}/{BOOKS_INDEX}/_mget"
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
                    "description": src.get("description", ""),
                    "genres": src.get("genres", ""),
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


def _call_llm(messages: list[dict[str, str]]) -> str:
    if not LLM_API_URL:
        return "LLM API not configured"
    headers = {"Content-Type": "application/json"}
    if LLM_API_KEY:
        headers["Authorization"] = f"Bearer {LLM_API_KEY}"
    payload = {
        "model": LLM_MODEL,
        "messages": messages,
        "max_tokens": LLM_MAX_TOKENS,
    }
    try:
        url = f"{LLM_API_URL}/chat/completions"
        resp = requests.post(url, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        log.warning("LLM call failed: %s", e)
        return f"Failed to get answer: {e}"


def _strip_markdown(text: str) -> str:
    text = re.sub(r"\*\*\*(.+?)\*\*\*", r"\1", text)
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"\*(.+?)\*", r"\1", text)
    text = re.sub(r"`(.+?)`", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"\n-{3,}\n", "\n", text)
    lines = [line.strip() for line in text.split("\n")]
    return "\n".join(lines).strip()


@app.post("/ask-book", response_model=AskBookResponse)
def ask_book(req: AskBookRequest) -> AskBookResponse:
    if not req.question.strip():
        return AskBookResponse(answer="", sources=[])
    question_lang = _detect_lang(req.question)
    meta = _fetch_book_metadata([req.book_id])
    book_meta = meta.get(str(req.book_id), {})
    meta_lines = []
    if book_meta.get("title"):
        meta_lines.append(f"Название: {book_meta['title']}")
    if book_meta.get("author"):
        meta_lines.append(f"Автор: {book_meta['author']}")
    if book_meta.get("description"):
        meta_lines.append(f"Описание: {book_meta['description']}")
    if book_meta.get("genres"):
        meta_lines.append(f"Жанры: {book_meta['genres']}")
    meta_text = "\n".join(meta_lines) if meta_lines else ""
    sr = ContentSearchRequest(query=req.question, book_id=req.book_id, size=req.top_k)
    paragraphs = search_content(sr)

    if not paragraphs or paragraphs[0].score < ASK_RELEVANCE_THRESHOLD:
        return AskBookResponse(
            answer=_get_no_relevant_msg(question_lang),
            sources=[]
        )

    relevance_check_prompt = (
        f"Book title: {book_meta.get('title', '')}\n"
        f"Book author: {book_meta.get('author', '')}\n"
        f"Book description: {book_meta.get('description', '')}\n\n"
        f"Is the following question related to this book? Answer ONLY 'yes' or 'no'.\n"
        f"Question: {req.question}"
    )
    relevance = _call_llm([
        {"role": "system", "content": "You are a relevance checker. Answer only 'yes' or 'no'."},
        {"role": "user", "content": relevance_check_prompt},
    ])
    if "no" in relevance.strip().lower()[:3]:
        return AskBookResponse(
            answer=_get_not_about_book_msg(question_lang),
            sources=[]
        )

    context_parts = []
    sources = []
    for p in paragraphs:
        ctx = f"[{p.chapter} | абзац {p.paragraph_index}]\n{p.text_snippet}"
        context_parts.append(ctx)
        sources.append({
            "book_id": p.book_id,
            "chapter": p.chapter,
            "chapter_index": p.chapter_index,
            "paragraph_index": p.paragraph_index,
            "text_snippet": p.text_snippet,
        })
    full_context_parts = []
    if meta_text:
        full_context_parts.append("Информация о книге:\n" + meta_text)
    if context_parts:
        full_context_parts.append("Отрывки из книги:\n" + "\n\n".join(context_parts))
    if not full_context_parts:
        return AskBookResponse(answer="No relevant content or metadata found for this book.", sources=[])
    full_context = "\n\n".join(full_context_parts)
    system_prompt = (
        "Ты — полезный ассистент, отвечающий на вопросы о книге. "
        "Используй информацию о книге (название, автор, описание, жанры) и предоставленные отрывки из текста, "
        "чтобы дать развёрнутый и точный ответ. Если отрывков недостаточно, опирайся на описание книги. "
        "Отвечай на языке вопроса."
    )
    user_prompt = f"{full_context}\n\nВопрос: {req.question}"
    answer = _call_llm([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ])
    answer = _strip_markdown(answer)
    return AskBookResponse(answer=answer, sources=sources)


@app.post("/search/reviews")
def search_reviews(req: ContentSearchRequest) -> List[Dict[str, Any]]:
    if not req.query.strip():
        return []
    query = req.query.strip()

    bm25_body: Dict[str, Any] = {
        "size": req.size or 20,
        "query": {"match": {"review_text": query}},
    }
    bm25_resp = es_post("reviews/_search", bm25_body)
    bm25_hits = bm25_resp.get("hits", {}).get("hits", [])

    bm25_results: List[Dict[str, Any]] = []
    for h in bm25_hits:
        src = h.get("_source", {})
        bm25_results.append({
            "book_id": src.get("book_id", ""),
            "review_text": src.get("review_text", ""),
            "score": float(h.get("_score", 0)),
        })

    knn_results: List[Dict[str, Any]] = []
    try:
        vec = embed(query)
        knn_body: Dict[str, Any] = {
            "size": req.size or 20,
            "knn": {
                "field": "review_text_vector",
                "query_vector": vec,
                "k": (req.size or 20),
                "num_candidates": max(100, (req.size or 20) * 10),
            }
        }
        knn_resp = es_post("reviews/_search", knn_body)
        knn_hits = knn_resp.get("hits", {}).get("hits", [])
        for h in knn_hits:
            src = h.get("_source", {})
            knn_results.append({
                "book_id": src.get("book_id", ""),
                "review_text": src.get("review_text", ""),
                "score": float(h.get("_score", 0)),
            })
    except HTTPException:
        log.warning("Embed/knn failed for review search; fallback to BM25 only")
    except Exception as e:
        log.warning("KNN failed for review search: %s", e)

    if not knn_results:
        return bm25_results

    r_bm25: Dict[str, int] = {}
    r_knn: Dict[str, int] = {}
    all_book_ids: set = set()
    for i, r in enumerate(bm25_results):
        bid = r["book_id"]
        r_bm25.setdefault(bid, i + 1)
        all_book_ids.add(bid)
    for i, r in enumerate(knn_results):
        bid = r["book_id"]
        r_knn.setdefault(bid, i + 1)
        all_book_ids.add(bid)

    scores: Dict[str, float] = {}
    texts: Dict[str, List[str]] = {}
    for bid in all_book_ids:
        rb = r_bm25.get(bid, 10**9)
        rk = r_knn.get(bid, 10**9)
        scores[bid] = 1.0 / (60 + rb) + 1.0 / (60 + rk)
        for r in bm25_results + knn_results:
            if r["book_id"] == bid:
                texts.setdefault(bid, []).append(r["review_text"])

    ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    results: List[Dict[str, Any]] = []
    for bid, _ in ordered[: (req.size or 20)]:
        reviews_for_book = texts.get(bid, [])
        if reviews_for_book:
            results.append({
                "book_id": bid,
                "reviews": reviews_for_book,
                "review_text": reviews_for_book[0],
                "review_count": len(reviews_for_book),
                "score": scores[bid],
            })

    if not results:
        results = bm25_results[: (req.size or 20)]

    return results


@app.post("/index/reviews")
def index_reviews(req: Dict[str, Any]) -> Dict[str, Any]:
    reviews = req.get("reviews", [])
    if not reviews:
        return {"status": "ok", "indexed": 0}

    if not _es_index_exists("reviews"):
        mapping = json.loads((MAP_DIR / "reviews.json").read_text(encoding="utf-8"))
        _es_put("reviews", mapping)
        log.info("Created Elasticsearch index: reviews")

    from elasticsearch import Elasticsearch, helpers
    es_client = Elasticsearch(
        ES_URL,
        headers={"Accept": "application/vnd.elasticsearch+json; compatible-with=8"}
    )

    actions: list[Dict] = []
    for rev in reviews:
        doc = {
            "review_id": rev.get("review_id"),
            "book_id": str(rev.get("book_id", "")),
            "user_id": rev.get("user_id"),
            "rating": rev.get("rating", 0.0),
            "review_text": rev.get("review_text", ""),
        }
        text = (rev.get("review_text") or "").strip()
        if text:
            try:
                vec = embed(text[:1000])
                doc["review_text_vector"] = vec
            except Exception:
                log.warning("Failed to embed review %s", rev.get("review_id"))
        action = {
            "_index": "reviews",
            "_id": str(rev.get("review_id")),
            "_source": doc,
        }
        actions.append(action)

    if actions:
        success, errors = helpers.bulk(es_client, actions, raise_on_error=False)
        indexed = success
        if errors:
            log.warning("Indexing errors: %s", errors[:5])
    else:
        indexed = 0

    return {"status": "ok", "indexed": indexed}


@app.get("/suggest")
def suggest(prefix: str = "", size: int = 10) -> List[Dict[str, Any]]:
    if not prefix.strip():
        return []
    body = {
        "suggest": {
            "book-suggest": {
                "prefix": prefix,
                "completion": {
                    "field": "suggest",
                    "size": size,
                    "skip_duplicates": True,
                }
            }
        }
    }
    resp = es_post(f"{BOOKS_INDEX}/_search", body)
    options = (
        resp
        .get("suggest", {})
        .get("book-suggest", [{}])[0]
        .get("options", [])
    )
    results: List[Dict[str, Any]] = []
    seen: set = set()
    for opt in options:
        text = opt.get("text", "")
        if text and text not in seen:
            seen.add(text)
            results.append({
                "text": text,
                "score": opt.get("_score", 0),
            })
    return results


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8001"))
    uvicorn.run("scripts.search_service:app", host="0.0.0.0", port=port, reload=False)