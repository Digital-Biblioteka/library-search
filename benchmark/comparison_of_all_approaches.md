# Comparison of all search approaches

## Configuration overview

| # | Configuration | Embedder | Dims | Reranker | KNN_K | TOP_N | RRF |
|---|-------------|----------|:----:|:--------:|:-----:|:-----:|:---:|
| A | minilm baseline | all-MiniLM-L6-v2 (local) | 384 | no | 100 | 20 | no |
| B | minilm + rerank | all-MiniLM-L6-v2 (local) | 384 | cross-encoder | 100 | 20 | no |
| C | api embed no rerank | text-embedding-3-small (polza ai) | 1536 | no | 100 | 20 | no |
| D | api embed + rerank | text-embedding-3-small (polza ai) | 1536 | cross-encoder | 100 | 20 | no |
| E | multilingual + rerank (best) | paraphrase-multilingual-MiniLM-L12-v2 (local) | 384 | cross-encoder | **300** | **30** | no |
| rrf idea 1 | python rrf fusion | text-embedding-3-small (polza ai) | 1536 | no | 100 | 20 | **python rrf k=60** |
| rrf final v2 | rrf + hard grouping (broken) | text-embedding-3-small (polza ai) | 1536 | no | 100 | 20 | **python rrf k=60 + lang grouping** |
| **rrf v3** | **rrf + fixes (current)** | text-embedding-3-small (polza ai) | 1536 | no | 100 | 20 | **python rrf k=60 + lang grouping + fixes** |

> note: configurations a-e were run through the java backend (`/api/search/books`), where bm25 + knn + optional reranker are merged at the java level.
>
> configurations rrf idea 1, rrf final v2, and rrf v3 were run through the python microservice (`/search/books`), where bm25 + knn are merged via python-level rrf (reciprocal rank fusion).
>
> "rrf final v2" had broken review search (wrong endpoint) and broken content search (wrong url). "rrf v3" is the current fixed version: review queries go to `/search/reviews`, content queries go to `/search/content`, plus fuzziness fixes, typo-tolerant cross-lingual matching, and single-word-only fuzzy matching to reduce noise.

---

## Overall metrics

| Metric | A | B | C | D | **E** | rrf idea 1 | rrf final v2 | **rrf v3** |
|:-------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| p@5 | 0.2531 | 0.2327 | 0.2980 | 0.3224 | **0.3347** | 0.2776 | 0.2816 | **0.4748** |
| r@5 | 0.5207 | 0.4738 | 0.5946 | 0.6602 | **0.6908** | 0.5574 | 0.5838 | 0.6074 |
| ndcg@5 | 0.5140 | 0.4717 | 0.5692 | 0.6223 | **0.6506** | 0.5698 | 0.5882 | 0.6113 |
| mrr | 0.6480 | 0.6248 | 0.6684 | 0.7007 | **0.7211** | 0.7014 | 0.6803 | **0.7616** |
| LangMatch@1 | — | — | — | — | — | — | — | **0.7959** |
| LangMatch@5 | — | — | — | — | — | — | — | **2.9184** |

**winner: rrf v3** by P@5 and MRR. E is still best for R@5.

The large P@5 improvement in rrf v3 (0.4748 vs 0.3347 for E) comes from fixing the broken review-search and content-search endpoints, plus fuzzy matching improvements that find exact title/author matches more consistently.

---

## By category

### bm25-author (4 queries)

| Metric | A | B | C | D | **E** | rrf idea 1 | rrf final v2 | **rrf v3** |
|:-------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| p@5 | 0.7000 | 0.4500 | 0.7000 | **0.9500** | **0.9500** | 0.6500 | 0.5000 | **1.0000** |
| r@5 | 0.6286 | 0.4500 | 0.7000 | **0.8786** | **0.8786** | 0.5786 | 0.4428 | **0.9286** |
| ndcg@5 | 0.7485 | 0.4985 | 0.7076 | **0.9576** | **0.9576** | 0.7120 | 0.4963 | **1.0000** |
| mrr | **1.0000** | 0.7500 | 0.7500 | **1.0000** | **1.0000** | **1.0000** | 0.6458 | **1.0000** |

### bm25-title-exact (9 queries)

| Metric | A | B | C | D | **E** | rrf idea 1 | rrf final v2 | **rrf v3** |
|:-------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| p@5 | 0.3111 | 0.3111 | 0.3556 | 0.3556 | 0.3556 | 0.3333 | 0.3556 | **0.9074** |
| r@5 | 0.8333 | 0.7963 | 0.9074 | 0.9444 | 0.9444 | 0.8889 | **0.9444** | **0.9444** |
| ndcg@5 | 0.8203 | 0.7869 | 0.8768 | 0.9085 | 0.9085 | 0.9140 | 0.9481 | **0.9644** |
| mrr | 0.8889 | 0.8889 | 0.9259 | 0.9259 | 0.9259 | **1.0000** | **1.0000** | **1.0000** |

### content-search (4 queries)

| Metric | A | B | C | D | **E** | rrf idea 1 | rrf final v2 | **rrf v3** |
|:-------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| p@5 | 0.0000 | 0.0000 | 0.0500 | 0.0500 | **0.2000** | 0.0000 | 0.0000 | 0.1000 |
| r@5 | 0.0000 | 0.0000 | 0.1250 | 0.1250 | **0.5000** | 0.0000 | 0.0000 | 0.2500 |
| ndcg@5 | 0.0000 | 0.0000 | 0.1533 | 0.1533 | **0.5000** | 0.0000 | 0.0000 | 0.2126 |
| mrr | 0.0000 | 0.0000 | 0.2500 | 0.2500 | **0.5000** | 0.0000 | 0.0000 | 0.3000 |

### cross-lingual-en-ru (5 queries)

| Metric | A | B | C | D | **E** | rrf idea 1 | rrf final v2 | **rrf v3** |
|:-------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| p@5 | 0.2400 | 0.2400 | **0.2400** | **0.2400** | **0.2400** | **0.2400** | **0.3200** | 0.2000 |
| r@5 | **0.7000** | **0.7000** | **0.7000** | **0.7000** | **0.7000** | **0.7000** | **0.9000** | 0.6000 |
| ndcg@5 | 0.7433 | 0.7433 | 0.7518 | 0.7518 | 0.7518 | 0.7066 | **0.8229** | 0.6292 |
| mrr | **1.0000** | **1.0000** | **1.0000** | **1.0000** | **1.0000** | 0.8667 | 0.9000 | 0.8667 |

### cross-lingual-ru-en (10 queries)

| Metric | A | B | C | D | **E** | rrf idea 1 | rrf final v2 | **rrf v3** |
|:-------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| p@5 | 0.2400 | 0.2400 | 0.3200 | 0.3200 | 0.3200 | 0.2200 | 0.2400 | **0.1400** |
| r@5 | 0.6500 | 0.6500 | 0.8333 | **0.9000** | **0.9000** | 0.6500 | 0.7000 | 0.4000 |
| ndcg@5 | 0.6109 | 0.6109 | 0.7909 | **0.8270** | **0.8270** | 0.6034 | 0.6920 | 0.3534 |
| mrr | 0.7250 | 0.7250 | **0.8333** | **0.8333** | **0.8333** | 0.6750 | 0.7000 | 0.5000 |

### filter-rating (4 queries)

| Metric | A | B | C | D | **E** | rrf idea 1 | rrf final v2 | **rrf v3** |
|:-------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| p@5 | 0.1000 | 0.1000 | 0.1500 | 0.1500 | 0.1500 | **0.3500** | **0.3500** | **0.8750** |
| r@5 | 0.2500 | 0.1667 | 0.2292 | 0.3125 | 0.3125 | **0.8750** | **0.8750** | 0.6875 |
| ndcg@5 | 0.2500 | 0.1913 | 0.2356 | 0.2895 | 0.2895 | **0.8610** | **0.8849** | 0.7129 |
| mrr | 0.2500 | 0.2500 | 0.3333 | 0.3333 | 0.3333 | **1.0000** | **1.0000** | **1.0000** |

### review-search (4 queries)

| Metric | A | B | C | D | **E** | rrf idea 1 | rrf final v2 | **rrf v3** |
|:-------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| p@5 | 0.1000 | 0.1000 | 0.0500 | **0.1000** | **0.1000** | 0.0000 | 0.0000 | **0.4000** |
| r@5 | **0.5000** | 0.3125 | 0.2500 | **0.5000** | **0.5000** | 0.0000 | 0.0000 | **0.8250** |
| ndcg@5 | **0.2827** | 0.1583 | 0.1077 | **0.2827** | **0.2827** | 0.0000 | 0.0000 | **0.7646** |
| mrr | **0.2083** | 0.1750 | 0.0625 | **0.2083** | **0.2083** | 0.0000 | 0.0000 | **0.8750** |

### semantic-same-language (9 queries)

| Metric | A | B | C | D | **E** | rrf idea 1 | rrf final v2 | **rrf v3** |
|:-------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| p@5 | 0.2667 | 0.2667 | 0.3556 | 0.3556 | 0.3556 | **0.3556** | **0.3556** | **0.3556** |
| r@5 | 0.2778 | 0.2593 | 0.4352 | **0.4537** | **0.4537** | 0.3889 | 0.3704 | 0.3889 |
| ndcg@5 | 0.3169 | 0.3124 | 0.3904 | **0.3949** | **0.3949** | **0.4262** | 0.4145 | **0.4262** |
| mrr | 0.6296 | 0.6296 | 0.6111 | 0.6111 | 0.6111 | **0.6981** | 0.6944 | **0.6981** |
