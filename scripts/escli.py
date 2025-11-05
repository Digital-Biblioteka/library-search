import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Iterable, List, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry

try:
    from sentence_transformers import SentenceTransformer
    import numpy as np
except Exception:
    SentenceTransformer = None
    np = None

ROOT = Path(__file__).resolve().parents[1]
MAP_DIR = ROOT / "mappings"
DEFAULT_ES = os.getenv("ES_URL", "http://localhost:9200")


def _session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def es_request(method: str, url: str, **kw):
    s = _session()
    r = s.request(method, url, timeout=60, **kw)
    if r.status_code >= 400:
        try:
            body = r.json()
        except Exception:
            body = r.text
        raise RuntimeError(f"ES {method} {url} -> {r.status_code}: {body}")
    return r


def init_indices(es: str):
    books = MAP_DIR / "books.json"
    content = MAP_DIR / "book_content.json"
    for name, path in ("books", books), ("book_content", content):
        # delete if exists
        es_request("DELETE", f"{es}/{name}") if index_exists(es, name) else None
        body = json.loads(path.read_text(encoding="utf-8"))
        es_request("PUT", f"{es}/{name}", json=body)
        print(f"OK created index {name}")


def index_exists(es: str, name: str) -> bool:
    r = requests.get(f"{es}/{name}")
    return r.status_code == 200


def bulk_file(es: str, ndjson_path: Path, chunk_bytes: int = 5 * 1024 * 1024):
    ndjson_path = Path(ndjson_path)
    buf: List[str] = []
    size = 0
    sent = 0
    with ndjson_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            buf.append(line)
            size += len(line.encode("utf-8"))
            if size >= chunk_bytes:
                _bulk_post(es, "".join(buf))
                sent += len(buf) // 2
                buf, size = [], 0
        if buf:
            _bulk_post(es, "".join(buf))
            sent += len(buf) // 2
    print(f"OK bulk indexed ~{sent} docs from {ndjson_path.name}")


def _bulk_post(es: str, data: str):
    r = es_request("POST", f"{es}/_bulk", data=data.encode("utf-8"), headers={"Content-Type": "application/x-ndjson"})
    resp = r.json()
    if resp.get("errors"):
        # show first error
        for item in resp.get("items", []):
            if any(k in item and item[k].get("error") for k in ("index", "update")):
                raise RuntimeError(f"Bulk error: {json.dumps(item, ensure_ascii=False)}")


def _load_model(model_name: str) -> SentenceTransformer:
    if SentenceTransformer is None:
        raise RuntimeError("sentence-transformers not installed. pip install -r scripts/requirements.txt")
    return SentenceTransformer(model_name)


def _iter_ndjson_docs(path: Path) -> Iterable[dict]:
    with Path(path).open("r", encoding="utf-8") as f:
        expect_src = False
        meta = None
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if "index" in obj:
                meta = obj
                expect_src = True
            else:
                if not expect_src:
                    continue
                yield meta, obj
                expect_src = False


def embed_from_ndjson(es: str, ndjson_path: Path, source_field: str, target_field: str, model_name: str = "sentence-transformers/all-MiniLM-L6-v2", index_override: str = None):
    model = _load_model(model_name)
    to_update: List[str] = []
    total = 0
    for meta, src in _iter_ndjson_docs(ndjson_path):
        idx = index_override or meta["index"]["_index"]
        if "_id" not in meta["index"]:
            text = src.get(source_field, "")
            if not text:
                continue
            vec = model.encode([text])[0].tolist()
            src[target_field] = vec
            to_update.append(json.dumps({"index": {"_index": idx}}))
            to_update.append(json.dumps(src, ensure_ascii=False))
        else:
            _id = meta["index"]["_id"]
            text = src.get(source_field, "")
            if not text:
                continue
            vec = model.encode([text])[0].tolist()
            to_update.append(json.dumps({"update": {"_index": idx, "_id": _id}}))
            to_update.append(json.dumps({"doc": {target_field: vec}}))
        if len(to_update) >= 1000:
            _bulk_post(es, "\n".join(to_update) + "\n")
            total += len(to_update) // 2
            to_update = []
    if to_update:
        _bulk_post(es, "\n".join(to_update) + "\n")
        total += len(to_update) // 2
    print(f"OK embedded+indexed ~{total} items from {Path(ndjson_path).name} -> field {target_field}")


def knn_test(es: str, index: str, field: str, query: str, k: int = 5, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
    model = _load_model(model_name)
    vec = model.encode([query])[0].tolist()
    body = {
        "knn": {
            "field": field,
            "query_vector": vec,
            "k": k,
            "num_candidates": max(100, k * 10)
        }
    }
    r = es_request("POST", f"{es}/{index}/_search", json=body)
    hits = r.json().get("hits", {}).get("hits", [])
    for h in hits:
        src = h.get("_source", {})
        score = h.get('_score', '')
        doc_id = h.get('_id', '')
        title = src.get("title") or src.get("chapter") or ""
        text_preview = (src.get("text", "") or "")[:80]
        text_preview = text_preview.replace("\n", " ")
        print(f"_score={score:.4f} id={doc_id} title={title} text={text_preview}")


def main():
    ap = argparse.ArgumentParser(description="ES CLI: indices, bulk, embeddings, kNN")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init-indices")
    p_init.add_argument("--es", default=DEFAULT_ES)

    p_bulk = sub.add_parser("bulk")
    p_bulk.add_argument("file", type=Path)
    p_bulk.add_argument("--es", default=DEFAULT_ES)

    p_embed = sub.add_parser("embed-from-ndjson")
    p_embed.add_argument("file", type=Path)
    p_embed.add_argument("--source-field", required=True, help="books: description; content: text")
    p_embed.add_argument("--target-field", required=True, help="books: description_vector; content: text_vector")
    p_embed.add_argument("--index-override", default=None, help="force index name when NDJSON has no _id/meta")
    p_embed.add_argument("--es", default=DEFAULT_ES)
    p_embed.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")

    p_knn = sub.add_parser("knn-test")
    p_knn.add_argument("--index", required=True)
    p_knn.add_argument("--field", required=True)
    p_knn.add_argument("--query", required=True)
    p_knn.add_argument("--k", type=int, default=5)
    p_knn.add_argument("--es", default=DEFAULT_ES)
    p_knn.add_argument("--model", default="sentence-transformers/all-MiniLM-L6-v2")

    args = ap.parse_args()

    if args.cmd == "init-indices":
        init_indices(args.es)
    elif args.cmd == "bulk":
        bulk_file(args.es, args.file)
    elif args.cmd == "embed-from-ndjson":
        embed_from_ndjson(args.es, args.file, args.source_field, args.target_field, args.model, args.index_override)
    elif args.cmd == "knn-test":
        knn_test(args.es, args.index, args.field, args.query, args.k, args.model)
    else:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
