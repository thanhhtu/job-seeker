FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

ARG TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        pip install --no-cache-dir torch==2.4.1; \
    else \
        pip install --no-cache-dir torch==2.4.1 --index-url https://download.pytorch.org/whl/cpu; \
    fi

RUN pip install --no-cache-dir "transformers==4.40.2" "FlagEmbedding==1.2.11"

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8001
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
