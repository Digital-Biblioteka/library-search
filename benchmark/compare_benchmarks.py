import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_FILE = os.path.join(SCRIPT_DIR, "_index.json")

CAT_ORDER = [
    "cross-lingual-ru-en",
    "cross-lingual-en-ru",
    "semantic-same-language",
    "bm25-title-exact",
    "bm25-author",
    "review-search",
    "content-search",
    "filter-rating",
    "overall",
]


def load_index() -> dict:
    if not os.path.exists(INDEX_FILE):
        print(f"Index file not found: {INDEX_FILE}", file=sys.stderr)
        print("Run run_benchmark.py first.", file=sys.stderr)
        sys.exit(1)
    with open(INDEX_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def load_results(label: str) -> dict:
    index = load_index()
    runs = {r["label"]: r for r in index["runs"]}
    if label not in runs:
        print(f"  [WARN] Label '{label}' not found in index.", file=sys.stderr)
        return None
    path = os.path.join(SCRIPT_DIR, runs[label]["result_file"])
    if not os.path.exists(path):
        print(f"  [WARN] Results not found for {label} at {path}", file=sys.stderr)
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def print_comparison(labels: list[str]) -> None:
    index = load_index()
    runs = {r["label"]: r for r in index["runs"]}

    results = {}
    for label in labels:
        if label not in runs:
            print(f"  [WARN] Label '{label}' not found in index. Skipping.",
                  file=sys.stderr)
            continue
        data = load_results(label)
        if data:
            results[label] = data

    if not results:
        print("No results to compare.", file=sys.stderr)
        return

    cat_metrics = {}
    for label, data in results.items():
        for cat_entry in data.get("by_category", []):
            cat = cat_entry["category"]
            if cat not in cat_metrics:
                cat_metrics[cat] = {}
            cat_metrics[cat][label] = cat_entry

    print("=" * 120)
    print("BENCHMARK COMPARISON TABLE")
    print("=" * 120)

    labels_list = list(results.keys())
    for label in labels_list:
        cfg = runs.get(label, {}).get("config", {})
        desc = runs.get(label, {}).get("description", "")
        print(f"  [{label}]: {desc}")
        print(f"           embed_mode={cfg.get('embed_mode','?')}, "
              f"model={cfg.get('embed_model','?')}, "
              f"dims={cfg.get('embed_dims','?')}, "
              f"rerank={cfg.get('rerank','?')}")
    print()

    col_w = 13
    header = f"{'Category':<28s}"
    for label in labels_list:
        header += (f"  {'P@5':>{col_w}s}{'R@5':>{col_w}s}"
                   f"{'N@5':>{col_w}s}{'MRR':>{col_w}s}")
    print(header)
    print("-" * len(header))

    for cat in CAT_ORDER:
        if cat not in cat_metrics:
            continue
        row = f"{cat:<28s}"
        for label in labels_list:
            entry = cat_metrics[cat].get(label)
            if entry:
                p = f"{entry.get('precision@5_mean', 0):.4f}"
                r = f"{entry.get('recall@5_mean', 0):.4f}"
                n = f"{entry.get('ndcg@5_mean', 0):.4f}"
                m = f"{entry.get('mrr', 0):.4f}"
                row += f"  {p:>{col_w}s}{r:>{col_w}s}{n:>{col_w}s}{m:>{col_w}s}"
            else:
                row += (f"  {'—':>{col_w}s}{'—':>{col_w}s}"
                        f"{'—':>{col_w}s}{'—':>{col_w}s}")
        print(row)

    print("=" * len(header))


def main():
    index = load_index()
    labels = [r["label"] for r in index["runs"]]

    if not labels:
        print("No benchmark runs found in index.", file=sys.stderr)
        return

    if len(sys.argv) > 1:
        labels = [l for l in labels if l in sys.argv[1:]]

    print_comparison(labels)


if __name__ == "__main__":
    main()