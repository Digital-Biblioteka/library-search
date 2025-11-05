import argparse
import json
import os
import re
import hashlib
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple

from ebooklib import epub, ITEM_DOCUMENT
from bs4 import BeautifulSoup
from minio import Minio
from minio.error import S3Error


def extract_texts_from_item(item) -> Tuple[str, List[str]]:
    soup = BeautifulSoup(item.get_content(), "lxml")
    chapter = None
    h = soup.find(["h1", "h2", "h3", "title"]) or {}
    if h:
        chapter = h.get_text(" ", strip=True)

    texts: List[str] = []
    for tag in soup.find_all(["p", "li", "pre"]):
        txt = tag.get_text("\n", strip=True)
        if txt:
            texts.append(txt)

    if not texts:
        body = soup.body.get_text("\n", strip=True) if soup.body else soup.get_text("\n", strip=True)
        chunks = [b.strip() for b in body.split("\n\n") if b.strip()]
        texts.extend(chunks)

    return chapter or item.get_name(), texts


essential_meta = [
    ("DC", "title", "title"),
    ("DC", "creator", "authors"),
    ("DC", "publisher", "publisher"),
    ("DC", "language", "language"),
    ("DC", "description", "description"),
    ("DC", "subject", "genres"),
]


def _pick_isbn(values: List[str]) -> str:
    for v in values:
        cleaned = v.replace(" ", "").replace("-", "")
        if re.fullmatch(r"\d{10}|\d{13}", cleaned):
            return cleaned


def _meta_values(book, ns: str, key: str) -> List[str]:
    try:
        pairs = book.get_metadata(ns, key)
    except Exception:
        pairs = []
    vals: List[str] = []
    for p in pairs:
        if isinstance(p, (list, tuple)) and p and isinstance(p[0], str):
            vals.append(p[0].strip())
    return vals


def _pick_stable_id(book, epub_path: Path) -> str:
    try:
        uid_attr = getattr(book, "uid", None)
    except Exception:
        uid_attr = None
    try:
        pairs = book.get_metadata("DC", "identifier")
    except Exception:
        pairs = []
    first_val = None
    for p in pairs:
        if isinstance(p, (list, tuple)) and p:
            val = str(p[0]).strip()
            attrs = p[1] if len(p) > 1 and isinstance(p[1], dict) else {}
            if not first_val and val:
                first_val = val
            if uid_attr and attrs.get("id") == uid_attr and val:
                return val
    if first_val:
        return first_val
    import hashlib
    h = hashlib.sha1()
    with open(epub_path, "rb") as rf:
        while True:
            chunk = rf.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def read_epub(epub_path: Path) -> Dict:
    book = epub.read_epub(str(epub_path))
    meta = book.metadata

    data: Dict = {
        "book_id": epub_path.stem,
        "chapters": [],
    }

    for ns, k, field in essential_meta:
        vals = _meta_values(book, ns, k)
        if field == "authors":
            data[field] = vals
        elif field == "genres":
            data[field] = ", ".join(vals) if vals else ""
        elif vals:
            data[field] = vals[0]

    if "authors" in data and isinstance(data["authors"], list):
        data["author"] = ", ".join([a for a in data["authors"] if a])
    else:
        data.setdefault("author", "")

    # derive source_uid and short book_id from it
    source_uid = _pick_stable_id(book, epub_path)
    data["source_uid"] = source_uid
    data["book_id"] = hashlib.sha1(source_uid.encode("utf-8")).hexdigest()[:16]

    data.setdefault("publisher", "")
    data.setdefault("genres", "")
    data.setdefault("linkToBook", "")
    if not data.get("title"):
        titles = _meta_values(book, "DC", "title")
        if titles:
            data["title"] = titles[0]
        else:
            data["title"] = epub_path.stem

    if "authors" in data:
        try:
            del data["authors"]
        except Exception:
            pass

    for item in book.get_items():
        if item.get_type() == ITEM_DOCUMENT:
            chapter, texts = extract_texts_from_item(item)
            if texts:
                data["chapters"].append({
                    "chapter": chapter,
                    "paragraphs": texts,
                })

    if not data.get("description"):
        paras: List[str] = []
        for ch in data.get("chapters", [])[:3]:
            paras.extend(ch.get("paragraphs", [])[:3])
        joined = "\n\n".join(paras).strip()
        if joined:
            data["description"] = joined[:2000]

    if not data.get("author") and data.get("chapters"):
        head = "\n".join(data["chapters"][0].get("paragraphs", [])[:10])
        m = re.search(r"\b[Bb]y\s+([^\n,]+)", head)
        if m:
            data["author"] = m.group(1).strip()
    if not data.get("publisher") and data.get("chapters"):
        head = "\n".join(data["chapters"][0].get("paragraphs", [])[:20])
        m = re.search(r"[Pp]ublisher:?\s*([^\n]+)", head)
        if m:
            data["publisher"] = m.group(1).strip()

    stem = epub_path.stem
    if not data.get("author"):
        author_slug = stem.split('_')[0].replace('-', ' ').strip()
        if author_slug and len(author_slug.split()) <= 4:
            data["author"] = author_slug.title()
    if not data.get("publisher") and data.get("chapters"):
        all_text = "\n".join(
            [p for ch in data.get("chapters", [])[:5] for p in ch.get("paragraphs", [])[:50]]
        )
        if "Standard Ebooks" in all_text:
            data["publisher"] = "Standard Ebooks"

    return data


def main():
    p = argparse.ArgumentParser(description="Extract EPUB into structured JSON per book")
    p.add_argument("input", nargs="?", type=Path, help="EPUB file or folder with EPUBs (local mode)")
    p.add_argument("output_dir", nargs="?", type=Path, help="Output directory for per-book JSON files (local mode)")
    p.add_argument("--s3", action="store_true", help="Process from MinIO: read RAW_BUCKET and write to PARSED_BUCKET")
    p.add_argument("--prefix", default="", help="S3 key prefix to filter EPUBs in RAW_BUCKET")
    args = p.parse_args()

    if args.s3:
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("INGEST_USER") or "ingest"
        secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("INGEST_PASSWORD") or "ingestpass"
        secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
        raw_bucket = os.getenv("RAW_BUCKET", "raw")
        parsed_bucket = os.getenv("PARSED_BUCKET", "parsed")

        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        try:
            if not client.bucket_exists(parsed_bucket):
                client.make_bucket(parsed_bucket)
        except S3Error:
            pass

        for obj in client.list_objects(raw_bucket, prefix=args.prefix, recursive=True):
            if not obj.object_name.lower().endswith(".epub"):
                continue
            try:
                with tempfile.TemporaryDirectory() as td:
                    tmp_path = Path(td) / Path(obj.object_name).name
                    response = client.get_object(raw_bucket, obj.object_name)
                    try:
                        with open(tmp_path, "wb") as w:
                            for d in response.stream(32 * 1024):
                                w.write(d)
                    finally:
                        response.close()
                        response.release_conn()

                    data = read_epub(tmp_path)
                    data["linkToBook"] = f"s3://{raw_bucket}/{obj.object_name}"
                    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
                    out_key = f"{Path(obj.object_name).with_suffix('.json').name}"
                    client.put_object(parsed_bucket, out_key, data=bytes(json_bytes), length=len(json_bytes))
                    print(f"OK s3://{raw_bucket}/{obj.object_name} -> s3://{parsed_bucket}/{out_key}")
            except Exception as e:
                print(f"ERROR s3://{raw_bucket}/{obj.object_name}: {e}")
        return

    if not args.input or not args.output_dir:
        p.error("For local mode provide input and output_dir, or use --s3")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    inputs: List[Path]
    if args.input.is_dir():
        inputs = sorted([p for p in args.input.glob("**/*.epub")])
    else:
        inputs = [args.input]

    for ep in inputs:
        try:
            data = read_epub(ep)
            try:
                data["linkToBook"] = f"file://{ep.resolve()}"
            except Exception:
                data["linkToBook"] = str(ep)
            out_path = args.output_dir / f"{ep.stem}.json"
            with out_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"OK {ep.name} -> {out_path}")
        except Exception as e:
            print(f"ERROR {ep}: {e}")


if __name__ == "__main__":
    main()
