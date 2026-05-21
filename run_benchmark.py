#!/usr/bin/env python3
"""
Search Benchmark Evaluation Script

Evaluates the search API at http://localhost:8080/api/search/books
using Information Retrieval metrics: Precision@5, Recall@5, NDCG@5, MRR.

Usage:
    python 2nd_sem/run_benchmark.py

Requires:
    - Docker running with search API at localhost:8080
    - 2nd_sem/benchmark_v2.json with 49 test queries
"""

import json
import math
import sys
import urllib.error
import urllib.parse
import urllib.request

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BENCHMARK_FILE = "/app/benchmark_v2.json"
RESULTS_FILE = "/app/benchmark_results.json"
SEARCH_API_URL = "http://library:8080/api/search/books"
TOP_K = 5
TOTAL_BOOKS = 115

# ---------------------------------------------------------------------------
# IR Metric Helpers
# ---------------------------------------------------------------------------


def compute_precision_at_5(relevant_ids, top_5_ids):
    """Precision@5 = |relevant ∩ top_5| / 5"""
    if not top_5_ids:
        return 0.0
    hits = sum(1 for bid in top_5_ids if bid in relevant_ids)
    return hits / 5.0


def compute_recall_at_5(relevant_ids, top_5_ids):
    """Recall@5 = |relevant ∩ top_5| / |all_relevant|"""
    if not relevant_ids:
        return 0.0
    hits = sum(1 for bid in top_5_ids if bid in relevant_ids)
    return hits / len(relevant_ids)


def dcg(relevances):
    """DCG = sum(rel_i / log2(i+1)) for i=1..n"""
    score = 0.0
    for i, rel in enumerate(relevances):
        score += rel / math.log2(i + 2)  # i+2 because i is 0-based
    return score


def compute_ndcg_at_5(id_to_rel, top_5_ids):
    """
    NDCG@5 = DCG@5 / IDCG@5.
    rel_i is the relevance score (0, 1, or 2) of the book at position i.
    Books not in the relevance map get score 0.
    """
    if not top_5_ids:
        return 0.0

    # Relevance scores at each of the top-5 positions in the returned order
    actual_rels = [id_to_rel.get(bid, 0) for bid in top_5_ids]

    # Ideal ordering: sort all known relevance scores descending,
    # pad with 0s up to top-5
    all_scores = sorted(id_to_rel.values(), reverse=True)
    ideal_rels = (all_scores + [0] * TOP_K)[:TOP_K]

    dcg_val = dcg(actual_rels)
    idcg_val = dcg(ideal_rels)

    if idcg_val == 0.0:
        return 0.0
    return dcg_val / idcg_val


def compute_reciprocal_rank(relevant_ids, top_5_ids):
    """
    Reciprocal rank of the first relevant result in top 5.
    Returns 0 if no relevant result found.
    """
    for rank, bid in enumerate(top_5_ids, start=1):
        if bid in relevant_ids:
            return 1.0 / rank
    return 0.0


# ---------------------------------------------------------------------------
# Search API Client
# ---------------------------------------------------------------------------


def build_request_body(query_entry):
    """Build the JSON body for the search API request."""
    body = {
        "query": query_entry["query"],
        "size": 10
    }

    # Add minRating filter if present
    if "minRating" in query_entry:
        body["minRating"] = query_entry["minRating"]

    # For review queries, also set reviewQuery to the same text
    if query_entry.get("type") == "review":
        body["reviewQuery"] = query_entry["query"]

    return body


def fetch_search_results(query_entry):
    """
    POST to search API and return the list of result book dicts.
    Returns an empty list on any error.
    """
    body = build_request_body(query_entry)
    data = json.dumps(body).encode("utf-8")

    req = urllib.request.Request(
        SEARCH_API_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            response_data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, OSError) as exc:
        print(f"  [WARN] API request failed for query "
              f"'{query_entry.get('id', '?')}': {exc}", file=sys.stderr)
        return []

    # The response may be a list directly or wrapped in an object with a
    # 'content' field (Spring Page wrapper). Handle both.
    if isinstance(response_data, list):
        return response_data
    elif isinstance(response_data, dict):
        # Try common Spring Page wrappers
        for key in ("content", "results", "data", "books"):
            if key in response_data and isinstance(response_data[key], list):
                return response_data[key]
    # If all else fails, assume it is a single-result dict and wrap it
    if isinstance(response_data, dict) and "id" in response_data:
        return [response_data]

    print(f"  [WARN] Unexpected response format for query "
          f"'{query_entry.get('id', '?')}'", file=sys.stderr)
    return []


def extract_id(book):
    """Extract the numeric book ID from a result dict."""
    for key in ("id", "bookId", "Id"):
        val = book.get(key)
        if val is not None:
            return int(val)
    return 0


def make_title(book):
    """Extract the title from a result dict."""
    for key in ("title", "Title", "bookTitle", "name", "bookName"):
        val = book.get(key)
        if val:
            return str(val)
    return "(unknown)"


# ---------------------------------------------------------------------------
# Benchmark Runner
# ---------------------------------------------------------------------------


def run_benchmark():
    """Run the full benchmark: load queries, call API, compute metrics, report."""

    # 1. Load benchmark queries
    with open(BENCHMARK_FILE, "r", encoding="utf-8") as fh:
        queries = json.load(fh)

    print(f"Loaded {len(queries)} queries from {BENCHMARK_FILE}", file=sys.stderr)

    # 2. Process each query
    per_query_results = []   # detailed results
    cat_results = {}         # category -> list of metric dicts

    for entry in queries:
        qid = entry["id"]
        cat = entry["category"]

        # Build relevance map: bookId -> relevance_score
        rel_map = {}
        rel_ids = set()

        if "relevanceMap" in entry:
            rmap = entry["relevanceMap"]
            book_ids = rmap.get("bookIds", [])
            scores = rmap.get("relevanceScores", [])
            for bid, score in zip(book_ids, scores):
                rel_map[bid] = score
                if score >= 1:
                    rel_ids.add(bid)

        # 3. Fetch search results
        results = fetch_search_results(entry)
        top_5 = results[:TOP_K]
        top_5_ids = [extract_id(b) for b in top_5]
        top_5_titles = [make_title(b) for b in top_5]

        # 4. Compute metrics
        p_at_5 = compute_precision_at_5(rel_ids, top_5_ids)
        r_at_5 = compute_recall_at_5(rel_ids, top_5_ids)
        ndcg = compute_ndcg_at_5(rel_map, top_5_ids)
        recip_rank = compute_reciprocal_rank(rel_ids, top_5_ids)

        # Store per-query results
        qres = {
            "id": qid,
            "category": cat,
            "query": entry["query"],
            "precision@5": round(p_at_5, 4),
            "recall@5": round(r_at_5, 4),
            "ndcg@5": round(ndcg, 4),
            "reciprocal_rank": round(recip_rank, 4),
            "top5_ids": top_5_ids,
            "top5_titles": top_5_titles,
        }
        per_query_results.append(qres)

        # Aggregate by category
        if cat not in cat_results:
            cat_results[cat] = []
        cat_results[cat].append(qres)

        # Print per-query line to stderr during processing
        print(f"  [{qid}] \"{entry['query'][:50]:50s}\""
              f"  P@5={p_at_5:.2f}  R@5={r_at_5:.2f}"
              f"  N@5={ndcg:.2f}  RR={recip_rank:.2f}",
              file=sys.stderr)

    # 5. Compute category summaries
    cat_summaries = []
    for cat in sorted(cat_results.keys()):
        qlist = cat_results[cat]
        n = len(qlist)
        ps = [q["precision@5"] for q in qlist]
        rs = [q["recall@5"] for q in qlist]
        ns = [q["ndcg@5"] for q in qlist]
        rrs = [q["reciprocal_rank"] for q in qlist]

        cat_summaries.append({
            "category": cat,
            "count": n,
            "precision@5_mean": round(sum(ps) / n, 4),
            "recall@5_mean": round(sum(rs) / n, 4),
            "ndcg@5_mean": round(sum(ns) / n, 4),
            "mrr": round(sum(rrs) / n, 4),
        })

    # 6. Overall summary
    n_all = len(per_query_results)
    overall = {
        "category": "overall",
        "count": n_all,
        "precision@5_mean": round(
            sum(q["precision@5"] for q in per_query_results) / n_all, 4),
        "recall@5_mean": round(
            sum(q["recall@5"] for q in per_query_results) / n_all, 4),
        "ndcg@5_mean": round(
            sum(q["ndcg@5"] for q in per_query_results) / n_all, 4),
        "mrr": round(
            sum(q["reciprocal_rank"] for q in per_query_results) / n_all, 4),
    }

    # 7. Build complete report
    report = {
        "metadata": {
            "total_queries": len(queries),
            "total_books": TOTAL_BOOKS,
            "benchmark_file": BENCHMARK_FILE,
            "api_url": SEARCH_API_URL,
            "top_k": TOP_K,
        },
        "per_query": per_query_results,
        "by_category": cat_summaries + [overall],
    }

    # Write JSON report
    with open(RESULTS_FILE, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
    print(f"\nResults saved to {RESULTS_FILE}", file=sys.stderr)

    # ------------------------------------------------------------------
    # 8. Print formatted tables to stdout
    # ------------------------------------------------------------------
    print_tables(queries, per_query_results, cat_summaries, overall)

    return report


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------


def print_tables(queries, per_query_results, cat_summaries, overall):
    """Print formatted results tables to stdout."""
    W = 70  # inner width

    # Header box
    print("+" + "=" * (W + 2) + "+")
    title = "SEARCH BENCHMARK RESULTS"
    print("|" + title.center(W + 2) + "|")
    print("+" + "=" * (W + 2) + "+")
    print(f"| Total queries: {len(queries):2d}  |  Total books: {TOTAL_BOOKS:<3d}"
          f"                  |")
    print("+" + "=" * (W + 2) + "+")
    print()

    # Per-category detail in a fixed order
    cat_order = [
        "cross-lingual-ru-en",
        "cross-lingual-en-ru",
        "semantic-same-language",
        "bm25-title-exact",
        "bm25-author",
        "review-search",
        "content-search",
        "filter-rating",
    ]

    for cat in cat_order:
        qlist = [q for q in per_query_results if q["category"] == cat]
        if not qlist:
            continue
        # Count total queries in this category from original file
        total_in_cat = sum(1 for e in queries if e["category"] == cat)
        print(f"{cat} ({total_in_cat} queries):")
        for q in qlist:
            print(
                f"  Query: \"{q['query']}\""
                f"  P@5={q['precision@5']:.2f}  R@5={q['recall@5']:.2f}"
                f"  NDCG@5={q['ndcg@5']:.2f}"
            )
        # Category summary
        cs = next((x for x in cat_summaries if x["category"] == cat), None)
        if cs:
            print(
                f"  -> {cat}:"
                f"  P@5={cs['precision@5_mean']:.2f}"
                f"  R@5={cs['recall@5_mean']:.2f}"
                f"  NDCG@5={cs['ndcg@5_mean']:.2f}"
                f"  MRR={cs['mrr']:.2f}"
            )
        print()

    # Overall summary box
    print("-" * (W + 4))
    o = overall
    print(
        f"  Overall:          "
        f"  P@5={o['precision@5_mean']:.2f}"
        f"  R@5={o['recall@5_mean']:.2f}"
        f"  NDCG@5={o['ndcg@5_mean']:.2f}"
        f"  MRR={o['mrr']:.2f}"
    )
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_benchmark()