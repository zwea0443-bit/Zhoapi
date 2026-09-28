FROM python:3.11-slim

# ═══ Install Tor + system deps ═══
RUN apt-get update && apt-get install -y \
    tor \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# ═══ Working directory ═══
WORKDIR /app

# ═══ Install Python deps ═══
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ═══ Copy app code ═══
COPY api.py .
COPY start.sh .
RUN chmod +x start.sh

# ═══ Expose port ═══
EXPOSE 8080

# ═══ Start ═══
CMD ["bash", "start.sh"]
