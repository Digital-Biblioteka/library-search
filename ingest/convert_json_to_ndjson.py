import argparse
import json
import os
import io
from pathlib import Path
from typing import Dict, List
from minio import Minio
from minio.error import S3Error


def to_bulk_actions(book_json: Dict, index_books: str, index_content: str) -> List[str]:
    actions: List[str] = []
    book_doc = {
        "book_id": book_json.get("book_id"),
        "source_uid": book_json.get("source_uid", ""),
        "title": book_json.get("title"),
        "author": book_json.get("author", ""),
        "publisher": book_json.get("publisher", ""),
        "description": book_json.get("description", ""),
        "genres": book_json.get("genres", ""),
        "linkToBook": book_json.get("linkToBook", ""),
        "language": book_json.get("language"),
        "suggest": {
            "input": ([book_json.get("title")] if book_json.get("title") else [])
                      + ([book_json.get("author")] if book_json.get("author") else [])
        }
    }
    actions.append(json.dumps({"index": {"_index": index_books, "_id": book_doc["book_id"]}}))
    actions.append(json.dumps(book_doc, ensure_ascii=False))

    seq = 0
    for chapter_index, ch in enumerate(book_json.get("chapters", [])):
        chapter = ch.get("chapter")
        for paragraph_index, para in enumerate(ch.get("paragraphs", [])):
            doc = {
                "book_id": book_json.get("book_id"),
                "chunk_id": f"{book_json.get('book_id')}-{seq:06d}",
                "chapter": chapter,
                "chapter_index": chapter_index,
                "paragraph_index": paragraph_index,
                "text": para,
            }
            actions.append(json.dumps({"index": {"_index": index_content}}))
            actions.append(json.dumps(doc, ensure_ascii=False))
            seq += 1

    return actions


def main():
    p = argparse.ArgumentParser(description="Convert per-book JSON to NDJSON bulk for ES")
    p.add_argument("input_dir", nargs="?", type=Path, help="Folder with per-book JSON files (local mode)")
    p.add_argument("out_books", nargs="?", type=Path, help="Output NDJSON for books index (local mode)")
    p.add_argument("out_content", nargs="?", type=Path, help="Output NDJSON for book_content index (local mode)")
    p.add_argument("--index-books", default="books")
    p.add_argument("--index-content", default="book_content")
    p.add_argument("--s3", action="store_true", help="Read JSONs from PARSED_BUCKET and write NDJSON files to INDEX_BUCKET")
    p.add_argument("--prefix", default="", help="S3 key prefix to filter JSONs in PARSED_BUCKET")
    args = p.parse_args()

    def process_json_objects(json_iter):
        books_lines: List[str] = []
        content_lines: List[str] = []
        for name, data in json_iter:
            try:
                actions = to_bulk_actions(data, args.index_books, args.index_content)
                for i in range(0, len(actions), 2):
                    meta = json.loads(actions[i])
                    src = actions[i+1]
                    idx = meta["index"]["_index"]
                    if idx == args.index_books:
                        books_lines.append(actions[i])
                        books_lines.append(src)
                    else:
                        meta_no_id = {"index": {"_index": args.index_content}}
                        content_lines.append(json.dumps(meta_no_id))
                        content_lines.append(src)
                print(f"OK {name}")
            except Exception as e:
                print(f"ERROR {name}: {e}")
        return books_lines, content_lines

    if args.s3:
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("INGEST_USER") or "ingest"
        secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("INGEST_PASSWORD") or "ingestpass"
        secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
        parsed_bucket = os.getenv("PARSED_BUCKET", "parsed")
        index_bucket = os.getenv("INDEX_BUCKET", "index")

        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

        def iter_json_from_s3():
            for obj in client.list_objects(parsed_bucket, prefix=args.prefix, recursive=True):
                if not obj.object_name.lower().endswith(".json"):
                    continue
                resp = client.get_object(parsed_bucket, obj.object_name)
                try:
                    buf = resp.read()
                finally:
                    resp.close(); resp.release_conn()
                try:
                    data = json.loads(buf.decode("utf-8"))
                except Exception as e:
                    print(f"ERROR decode {obj.object_name}: {e}")
                    continue
                yield obj.object_name, data

        books_lines, content_lines = process_json_objects(iter_json_from_s3())

        books_bytes = ("\n".join(books_lines) + "\n").encode("utf-8")
        content_bytes = ("\n".join(content_lines) + "\n").encode("utf-8")
        client.put_object(index_bucket, "books.ndjson", data=io.BytesIO(books_bytes), length=len(books_bytes))
        client.put_object(index_bucket, "book_content.ndjson", data=io.BytesIO(content_bytes), length=len(content_bytes))
        print(f"S3 write: s3://{index_bucket}/books.ndjson and book_content.ndjson")
        return

    if not args.input_dir or not args.out_books or not args.out_content:
        p.error("For local mode provide input_dir out_books out_content, or use --s3")

    def iter_json_local():
        for f in sorted(args.input_dir.glob("*.json")):
            data = json.loads(f.read_text(encoding="utf-8"))
            yield f.name, data

    books_lines, content_lines = process_json_objects(iter_json_local())

    args.out_books.parent.mkdir(parents=True, exist_ok=True)
    args.out_books.write_text("\n".join(books_lines) + "\n", encoding="utf-8")
    args.out_content.parent.mkdir(parents=True, exist_ok=True)
    args.out_content.write_text("\n".join(content_lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
