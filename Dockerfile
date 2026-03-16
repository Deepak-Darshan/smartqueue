FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Create non-root user early
RUN adduser --disabled-password --gecos "" appuser

# Install dependencies first — separate layer so code changes don't bust this cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only what the producer needs
COPY producer/ ./producer/
COPY shared/ ./shared/

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["uvicorn", "producer.main:app", "--host", "0.0.0.0", "--port", "8000"]
