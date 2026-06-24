# ─────────────────────────────────────────────
# Stage 1: builder — ставим зависимости
# ─────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Системные зависимости для компиляции (lxml, etc.)
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

# Копируем только файл зависимостей — слой кешируется пока requirements не изменится
COPY scripts/requirements_search.txt ./requirements.txt

RUN pip install --upgrade pip \
    && pip install --no-cache-dir --prefer-binary --user -r requirements.txt

# ─────────────────────────────────────────────
# Stage 2: runtime — минимальный финальный образ
# ─────────────────────────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

# curl нужен для healthcheck
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Переносим установленные пакеты из builder-слоя (без gcc и прочего build-мусора)
COPY --from=builder /root/.local /root/.local

# Копируем только нужный код — scripts, ingest (импортируется search_service), mappings
COPY scripts/   ./scripts/
COPY ingest/    ./ingest/
COPY mappings/  ./mappings/

# Переменные окружения по умолчанию (переопределяются в docker-compose / env_file)
ENV PATH=/root/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    ES_URL=http://elasticsearch:9200 \
    EMBED_URL=http://embed:8000/embed \
    PORT=8001

EXPOSE 8001

HEALTHCHECK --interval=10s --timeout=5s --retries=10 --start-period=60s \
    CMD curl -fsS http://localhost:8001/healthz || exit 1

CMD ["uvicorn", "scripts.search_service:app", "--host", "0.0.0.0", "--port", "8001", "--log-level", "warning"]
