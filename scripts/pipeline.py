import argparse
import os
import sys
import tempfile
import subprocess
from pathlib import Path
from typing import Optional, List

from minio import Minio
from minio.error import S3Error

ROOT = Path(__file__).resolve().parents[1]
ESCLI = ROOT / "scripts" / "escli.py"
EPUB_TO_JSON = ROOT / "ingest" / "epub_to_json.py"
JSON_TO_NDJSON = ROOT / "ingest" / "convert_json_to_ndjson.py"


def get_env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name)
    return v if v is not None else default


def minio_client() -> Minio:
    endpoint = get_env("MINIO_ENDPOINT", "localhost:9000")
    access_key = get_env("MINIO_ACCESS_KEY") or get_env("INGEST_USER") or "minioadmin"
    secret_key = get_env("MINIO_SECRET_KEY") or get_env("INGEST_PASSWORD") or "minioadmin"
    secure = (get_env("MINIO_SECURE", "false").lower() == "true")
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def cmd_upload_epubs(args):
    client = minio_client()
    raw_bucket = get_env("RAW_BUCKET", "raw")
    prefix = args.prefix.strip("/") if args.prefix else ""
    src = Path(args.dir)
    if not src.exists():
        raise SystemExit(f"Directory not found: {src}")

    try:
        if not client.bucket_exists(raw_bucket):
            client.make_bucket(raw_bucket)
    except S3Error:
        pass

    total = 0
    for p in src.rglob("*.epub"):
        rel = p.relative_to(src).as_posix()
        key = f"{prefix}/{rel}" if prefix else rel
        key = key.replace("\\", "/")
        client.fput_object(raw_bucket, key, str(p))
        print(f"UPLOADED {p} -> s3://{raw_bucket}/{key}")
        total += 1
    print(f"OK uploaded {total} epubs to s3://{raw_bucket}/{prefix or ''}")


def _run_py(module_path: Path, argv: List[str], extra_env: Optional[dict] = None):
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    cmd = [sys.executable, str(module_path)] + argv
    proc = subprocess.run(cmd, env=env)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def cmd_ingest_s3(args):
    run_args = ["--s3"]
    if args.prefix:
        run_args += ["--prefix", args.prefix]
    _run_py(EPUB_TO_JSON, run_args)


def cmd_json_to_ndjson_s3(args):
    run_args = ["--s3"]
    if args.prefix:
        run_args += ["--prefix", args.prefix]
    if args.index_books:
        run_args += ["--index-books", args.index_books]
    if args.index_content:
        run_args += ["--index-content", args.index_content]
    _run_py(JSON_TO_NDJSON, run_args)


def cmd_es_bulk_from_minio(args):
    client = minio_client()
    index_bucket = get_env("INDEX_BUCKET", "index")

    books_key = args.books_key
    content_key = args.content_key
    es_url = args.es

    def _download(key: str, dst: Path):
        resp = client.get_object(index_bucket, key)
        try:
            data = resp.read()
        finally:
            resp.close(); resp.release_conn()
        dst.write_bytes(data)
        print(f"DOWNLOADED s3://{index_bucket}/{key} -> {dst}")

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        books_path = td_path / Path(books_key).name
        content_path = td_path / Path(content_key).name

        _download(books_key, books_path)
        _download(content_key, content_path)

        _run_py(ESCLI, ["init-indices", "--es", es_url])
        _run_py(ESCLI, ["bulk", str(books_path), "--es", es_url])
        _run_py(ESCLI, ["bulk", str(content_path), "--es", es_url])


def cmd_embed_from_minio(args):
    client = minio_client()
    index_bucket = get_env("INDEX_BUCKET", "index")

    ndjson_key = args.ndjson_key
    es_url = args.es
    source_field = args.source_field
    target_field = args.target_field
    model = args.model

    with tempfile.TemporaryDirectory() as td:
        td_path = Path(td)
        local_path = td_path / Path(ndjson_key).name

        # download NDJSON from MinIO
        resp = client.get_object(index_bucket, ndjson_key)
        try:
            data = resp.read()
        finally:
            resp.close(); resp.release_conn()
        local_path.write_bytes(data)
        print(f"DOWNLOADED s3://{index_bucket}/{ndjson_key} -> {local_path}")

        _run_py(ESCLI, [
            "embed-from-ndjson",
            str(local_path),
            "--source-field", source_field,
            "--target-field", target_field,
            "--es", es_url,
            "--model", model,
        ])


def main():
    ap = argparse.ArgumentParser(description="Library pipeline tools")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_up = sub.add_parser("upload-epubs", help="Upload local EPUBs to MinIO raw bucket")
    p_up.add_argument("dir", help="Local directory with .epub files (recursively)")
    p_up.add_argument("--prefix", default="", help="Key prefix in raw bucket")
    p_up.set_defaults(func=cmd_upload_epubs)

    p_ing = sub.add_parser("ingest-s3", help="Run epub_to_json.py --s3 over RAW_BUCKET -> PARSED_BUCKET")
    p_ing.add_argument("--prefix", default="", help="Filter RAW_BUCKET by prefix")
    p_ing.set_defaults(func=cmd_ingest_s3)

    p_conv = sub.add_parser("json-to-ndjson-s3", help="Run convert_json_to_ndjson.py --s3 PARSED_BUCKET -> INDEX_BUCKET")
    p_conv.add_argument("--prefix", default="", help="Filter PARSED_BUCKET by prefix")
    p_conv.add_argument("--index-books", default="books", help="Books index name")
    p_conv.add_argument("--index-content", default="book_content", help="Content index name")
    p_conv.set_defaults(func=cmd_json_to_ndjson_s3)

    p_bulk = sub.add_parser("es-bulk-from-minio", help="Download NDJSON from INDEX_BUCKET and bulk load to ES")
    p_bulk.add_argument("--es", default=os.getenv("ES_URL", "http://localhost:9200"))
    p_bulk.add_argument("--books-key", default="books.ndjson")
    p_bulk.add_argument("--content-key", default="book_content.ndjson")
    p_bulk.set_defaults(func=cmd_es_bulk_from_minio)

    p_embed = sub.add_parser("embed-from-minio", help="Download NDJSON from INDEX_BUCKET and embed+index to ES")
    p_embed.add_argument("--ndjson-key", default="book_content.ndjson", help="Key in INDEX_BUCKET: books.ndjson or book_content.ndjson")
    p_embed.add_argument("--source-field", required=True, help="Source text field in NDJSON (e.g., text or description)")
    p_embed.add_argument("--target-field", required=True, help="Target vector field in ES (e.g., text_vector or description_vector)")
    p_embed.add_argument("--model", default="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
    p_embed.add_argument("--es", default=os.getenv("ES_URL", "http://localhost:9200"))
    p_embed.set_defaults(func=cmd_embed_from_minio)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
