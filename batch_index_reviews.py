#!/usr/bin/env python3
import subprocess, json, sys, urllib.request

import os
os.environ["PGPASSWORD"] = "postgres"

sql = "SELECT id, book_id, user_id, rating, review_text FROM reviews ORDER BY id"
cmd = ["psql", "-U", "postgres", "-d", "library", "-h", "db", "-A", "-t", "-F", "|", "-c", sql]

try:
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
except FileNotFoundError:
    cmd[0] = "/usr/bin/psql"
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

if result.returncode != 0:
    print(f"psql error: {result.stderr}", file=sys.stderr)
    sys.exit(1)

lines = result.stdout.strip().split("\n")
reviews = []
for line in lines:
    line = line.strip()
    if not line:
        continue
    parts = line.split("|")
    if len(parts) < 5:
        continue
    try:
        reviews.append({
            "review_id": int(parts[0]),
            "book_id": parts[1],
            "user_id": int(parts[2]) if parts[2] else 0,
            "rating": float(parts[3]) if parts[3] else 0.0,
            "review_text": parts[4],
        })
    except ValueError:
        continue

print(f"Fetched {len(reviews)} reviews from DB", file=sys.stderr)

if not reviews:
    print("No reviews to index", file=sys.stderr)
    sys.exit(0)

INDEX_URL = "http://localhost:8001/index/reviews"
batch_size = 50
total_indexed = 0
for i in range(0, len(reviews), batch_size):
    batch = reviews[i:i + batch_size]
    body = json.dumps({"reviews": batch}).encode("utf-8")
    req = urllib.request.Request(
        INDEX_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            resp_data = json.loads(resp.read().decode("utf-8"))
            indexed = resp_data.get("indexed", 0)
            total_indexed += indexed
            print(f"  Batch {i // batch_size + 1}: indexed {indexed}", file=sys.stderr)
    except Exception as e:
        print(f"  Batch {i // batch_size + 1} failed: {e}", file=sys.stderr)

print(f"\nTotal indexed: {total_indexed} reviews", file=sys.stderr)