FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src \
    SQL_DIR=/app/sql

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ ./src/
COPY sql/ ./sql/
COPY config/ ./config/

RUN useradd --system --uid 10001 emulator

USER emulator

ENTRYPOINT ["python", "-m", "emulator"]
CMD ["run"]
