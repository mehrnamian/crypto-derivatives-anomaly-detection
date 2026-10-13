FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DERIVS_DATA_DIR=/data

RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid app --home-dir /app app
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --upgrade pip && python -m pip install .

USER 10001:10001
EXPOSE 8000
ENTRYPOINT ["derivs-anomaly"]
CMD ["--help"]
