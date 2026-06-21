import json
import math
import sys
import urllib.error
import urllib.request
import os

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BENCHMARK_FILE = os.path.join(_SCRIPT_DIR, "benchmark_v2.json")
RESULTS_FILE = os.path.join(_SCRIPT_DIR, "benchmark_rrf_results.json")
SEARCH_API_URL = "http://localhost:8001/search/books"
CONTENT_SEARCH_API_URL = "http://localhost:8001/search/content"
SEMANTIC_SEARCH_API_URL = "http://localhost:8001/search/books/semantic"
REVIEW_SEARCH_API_URL = "http://localhost:8001/search/reviews"
TOP_K = 5
TOTAL_BOOKS = 99


def compute_precision_at_5(relevant_ids, top_5_ids):
    if not top_5_ids:
        return 0.0
    hits = sum(1 for bid in top_5_ids if bid in relevant_ids)
    return hits / len(top_5_ids)

def compute_recall_at_5(relevant_ids, top_5_ids):
    if not relevant_ids:
        return 0.0
    hits = sum(1 for bid in top_5_ids if bid in relevant_ids)
    return hits / len(relevant_ids)

def dcg(relevances):
    return sum(rel / math.log2(i + 2) for i, rel in enumerate(relevances))

def compute_ndcg_at_5(id_to_rel, top_5_ids):
    if not top_5_ids:
        return 0.0
    actual_rels = [id_to_rel.get(bid, 0) for bid in top_5_ids]
    all_scores = sorted(id_to_rel.values(), reverse=True)
    ideal_rels = (all_scores + [0] * TOP_K)[:TOP_K]
    dcg_val = dcg(actual_rels)
    idcg_val = dcg(ideal_rels)
    if idcg_val == 0.0:
        return 0.0
    return dcg_val / idcg_val

def compute_reciprocal_rank(relevant_ids, top_5_ids):
    for rank, bid in enumerate(top_5_ids, start=1):
        if bid in relevant_ids:
            return 1.0 / rank
    return 0.0

def _detect_lang(text: str) -> str:
    for ch in text:
        if 'а' <= ch.lower() <= 'я':
            return "russian"
    return "english"

def build_request_body(query_entry):
    qtype = query_entry.get("type", "bm25")
    if qtype == "content":
        body = {"query": query_entry["query"], "size": 10}
    else:
        # bm25, semantic, review, filter all use /search endpoint fields
        body = {"query": query_entry["query"], "size": 10}
    if "minRating" in query_entry:
        body["minRating"] = query_entry["minRating"]
    return body

def fetch_search_results(query_entry):
    qtype = query_entry.get("type", "bm25")
    body = build_request_body(query_entry)
    data = json.dumps(body).encode("utf-8")
    if qtype == "content":
        api_url = CONTENT_SEARCH_API_URL
    elif qtype == "semantic":
        api_url = SEMANTIC_SEARCH_API_URL
    elif qtype == "review":
        api_url = REVIEW_SEARCH_API_URL
    else:
        api_url = SEARCH_API_URL
    req = urllib.request.Request(
        api_url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            response_data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, urllib.error.HTTPError,
            json.JSONDecodeError, OSError) as exc:
        print(f"  [WARN] API request failed for query "
              f"'{query_entry.get('id', '?')}': {exc}", file=sys.stderr)
        return []
    if isinstance(response_data, list):
        return response_data
    elif isinstance(response_data, dict):
        for key in ("content", "results", "data", "books"):
            if key in response_data and isinstance(response_data[key], list):
                return response_data[key]
    if isinstance(response_data, dict) and "id" in response_data:
        return [response_data]
    print(f"  [WARN] Unexpected response format for query "
          f"'{query_entry.get('id', '?')}'", file=sys.stderr)
    return []

def extract_id(book):
    if isinstance(book, dict):
        for key in ("book_id", "id", "bookId"):
            if key in book:
                val = book[key]
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return val
    return None

def make_title(book):
    if isinstance(book, dict):
        for key in ("title", "book_title", "bookTitle"):
            if key in book and book[key]:
                return book[key]
        name = book.get("name", "")
        if "cover_text" in book and isinstance(book["cover_text"], str):
            title_line = book["cover_text"].split("\n")[0]
            return title_line[:60] if title_line else name[:60]
        return str(name)[:60] if name else "?"
    return "?"

with open(BENCHMARK_FILE, "r", encoding="utf-8") as f:
    queries = json.load(f)

per_query_results = []
cat_buckets = {}

for qi, entry in enumerate(queries, start=1):
    qid = entry.get("id", f"q{qi}")
    qtype = entry.get("type", "bm25")
    query_text = entry.get("query", "")
    desc = entry.get("description", "")
    cat = entry.get("category", qtype)
    relevance_map_raw = entry.get("relevanceMap", {})
    relevant_ids = set(entry.get("relevantIds", []))
    if not relevant_ids and "bookIds" in relevance_map_raw:
        relevant_ids = set(relevance_map_raw["bookIds"])
    relevance_map = {}
    if "bookIds" in relevance_map_raw and "relevanceScores" in relevance_map_raw:
        for bid, score in zip(relevance_map_raw["bookIds"], relevance_map_raw["relevanceScores"]):
            relevance_map[int(bid)] = score
    else:
        relevance_map = {int(k): v for k, v in relevance_map_raw.items() if k != "bookIds" and k != "relevanceScores"}
    min_rating = entry.get("minRating")
    if min_rating is not None:
        qtype = "filter-rating"
    print(f"[{qi}/{len(queries)}] {qid} ({qtype}): {query_text[:60]}", end="", flush=True)
    results = fetch_search_results(entry)
    top5 = results[:5]
    top5_ids = [extract_id(b) for b in top5 if extract_id(b) is not None]
    top5_titles = [make_title(b) for b in top5]
    top5_languages = [b.get("language") if isinstance(b, dict) else None for b in top5]
    query_lang = _detect_lang(query_text) if query_text else "english"
    lang_match_1 = 1 if (top5_languages and top5_languages[0] and top5_languages[0].lower() == query_lang.lower()) else 0
    lang_match_5 = sum(1 for lang in top5_languages if lang and lang.lower() == query_lang.lower())
    p5 = compute_precision_at_5(relevant_ids, top5_ids)
    r5 = compute_recall_at_5(relevant_ids, top5_ids)
    ndcg5 = compute_ndcg_at_5(relevance_map, top5_ids)
    mrr = compute_reciprocal_rank(relevant_ids, top5_ids)
    per_query_results.append({
        "id": qid,
        "type": qtype,
        "query": query_text,
        "description": desc,
        "category": cat,
        "top5_ids": top5_ids,
        "top5_titles": top5_titles,
        "top5_languages": top5_languages,
        "query_language": query_lang,
        "P@5": round(p5, 4),
        "R@5": round(r5, 4),
        "NDCG@5": round(ndcg5, 4),
        "MRR": round(mrr, 4),
        "LangMatch@1": lang_match_1,
        "LangMatch@5": lang_match_5,
    })
    print(f" P@5={p5:.2f} R@5={r5:.2f} NDCG@5={ndcg5:.2f} MRR={mrr:.2f}"
          f" LangM@1={lang_match_1} LangM@5={lang_match_5}/{TOP_K}")
    cat_buckets.setdefault(cat, []).append(per_query_results[-1])

overall = {
    "P@5": round(sum(q["P@5"] for q in per_query_results) / len(per_query_results), 4),
    "R@5": round(sum(q["R@5"] for q in per_query_results) / len(per_query_results), 4),
    "NDCG@5": round(sum(q["NDCG@5"] for q in per_query_results) / len(per_query_results), 4),
    "MRR": round(sum(q["MRR"] for q in per_query_results) / len(per_query_results), 4),
    "LangMatch@1": round(sum(q["LangMatch@1"] for q in per_query_results) / len(per_query_results), 4),
    "LangMatch@5": round(sum(q["LangMatch@5"] for q in per_query_results) / len(per_query_results), 4),
}

by_category = []
for cat, cat_results in sorted(cat_buckets.items()):
    by_category.append({
        "category": cat.replace("-", " ").title(),
        "count": len(cat_results),
        "P@5": round(sum(q["P@5"] for q in cat_results) / len(cat_results), 4),
        "R@5": round(sum(q["R@5"] for q in cat_results) / len(cat_results), 4),
        "NDCG@5": round(sum(q["NDCG@5"] for q in cat_results) / len(cat_results), 4),
        "MRR": round(sum(q["MRR"] for q in cat_results) / len(cat_results), 4),
        "LangMatch@1": round(sum(q["LangMatch@1"] for q in cat_results) / len(cat_results), 4),
        "LangMatch@5": round(sum(q["LangMatch@5"] for q in cat_results) / len(cat_results), 4),
    })

print("\n" + "=" * 96)
print("OVERALL RESULTS (RRF hybrid + Language Preference)")
print("=" * 96)
print(f"  P@5          = {overall['P@5']:.4f}")
print(f"  R@5          = {overall['R@5']:.4f}")
print(f"  NDCG@5       = {overall['NDCG@5']:.4f}")
print(f"  MRR          = {overall['MRR']:.4f}")
print(f"  LangMatch@1  = {overall['LangMatch@1']:.4f}")
print(f"  LangMatch@5  = {overall['LangMatch@5']:.4f}")
print("=" * 96)
print("\nBY CATEGORY:")
for cat in by_category:
    print(f"  {cat['category']:30s} | count={cat['count']:2d} | "
          f"P@5={cat['P@5']:.4f} R@5={cat['R@5']:.4f} "
          f"NDCG@5={cat['NDCG@5']:.4f} MRR={cat['MRR']:.4f} "
          f"LangM@1={cat['LangMatch@1']:.4f} LangM@5={cat['LangMatch@5']:.4f}")

result_data = {
    "metadata": {
        "label": "RRF hybrid (Idea 1)",
        "description": "BM25 + KNN + Python-level RRF fusion on /search/books endpoint",
        "overall": overall,
        "by_category": by_category,
    },
    "per_query": per_query_results,
}

with open(RESULTS_FILE, "w", encoding="utf-8") as f:
    json.dump(result_data, f, ensure_ascii=False, indent=2)
print(f"\nResults saved to {RESULTS_FILE}")