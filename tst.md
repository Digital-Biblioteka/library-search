This doc shows how to run the full pipeline: MinIO ingest -> build NDJSON -> load into Elasticsearch -> verify search

## Prerequisites
- Docker Desktop running
- Python 3.8+ with venv

## 1) Start Elasticsearch + Kibana
From this repo folder:

```bash
docker compose up -d
# cerify ES is up
# http://localhost:9200 should respond with JSON
```

## 2) Prepare MinIO data (EPUB -> JSON in MinIO)
If you already have JSON in MinIO bucket `parsed`, skip to step 3

1) Ensure MinIO is running (see the library-backend repo tst.md to bring it up quickly)
2) Upload `*.epub` to bucket `raw` via MinIO Console (http://localhost:9001)
3) Create venv and install deps:
```bash
python -m venv .venv-search
.\.venv-search\Scripts\Activate.ps1
pip install -r .\scripts\requirements.txt
pip install minio
```
4) Run ingest from MinIO to MinIO:
```bash
$env:MINIO_ENDPOINT="localhost:9000"
$env:MINIO_ACCESS_KEY="minioadmin"
$env:MINIO_SECRET_KEY="minioadmin"
$env:RAW_BUCKET="raw"
$env:PARSED_BUCKET="parsed"
$env:MINIO_SECURE="false"
python .\ingest\epub_to_json.py --s3
```
Result: JSON per book created in MinIO bucket `parsed`.

## 3) Download JSON from MinIO (optional organization step)
Download all `*.json` from bucket `parsed` into:
```
.ingest_out\json
```

## 4) Convert JSON -> NDJSON
```bash
python .\ingest\convert_json_to_ndjson.py .\ingest_out\json .\ingest_out\books_out.ndjson .\ingest_out\book_content_out.ndjson
```
Outputs:
- `ingest_out\books_out.ndjson`
- `ingest_out\book_content_out.ndjson`

## 5) Initialize indices and bulk load
```bash
python .\scripts\escli.py init-indices --es http://localhost:9200
python .\scripts\escli.py bulk --es http://localhost:9200 .\ingest_out\books_out.ndjson
python .\scripts\escli.py bulk --es http://localhost:9200 .\ingest_out\book_content_out.ndjson
```
Verify:
- `http://localhost:9200/books/_count`
- `http://localhost:9200/book_content/_count`

## 6) Create embeddings for kNN
To use kNN later with a local model:
```bash
pip install -r .\scripts\requirements.txt
python .\scripts\escli.py embed-from-ndjson --es http://localhost:9200 ^
  .\ingest_out\book_content_out.ndjson ^
  --source-field text --target-field text_vector
```