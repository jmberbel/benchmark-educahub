FROM python:3.11-slim-bookworm

WORKDIR /app

# System deps for Playwright + matplotlib
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl gnupg \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 \
    libasound2 libpango-1.0-0 libcairo2 \
    fonts-dejavu-core fonts-liberation fonts-unifont \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium

# Cache bust: v3 - 2026-03-05
COPY . .

# Create persistent directories
RUN mkdir -p /app/uploads /app/outputs /app/sessions

# Volumes for persistent data (map these in Easypanel)
VOLUME ["/app/uploads", "/app/outputs", "/app/sessions"]

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
