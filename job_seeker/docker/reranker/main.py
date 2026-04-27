from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from FlagEmbedding import FlagReranker
from transformers import AutoTokenizer
from typing import List
import logging
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

model = FlagReranker('BAAI/bge-reranker-v2-m3', use_fp16=True)

model.tokenizer = AutoTokenizer.from_pretrained(
    'BAAI/bge-reranker-v2-m3',
    use_fast=True 
)

class RerankRequest(BaseModel):
    query: str
    documents: List[str]

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/rerank")
def rerank(request: RerankRequest):
    try:
        pairs = [[request.query, doc] for doc in request.documents]
        scores = model.compute_score(pairs)

        if isinstance(scores, (float, int, np.floating)):
            scores = [float(scores)]
        else:
            scores = [float(s) for s in scores]

        return {"scores": scores}
    except Exception as e:
        logger.exception(f"Rerank failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    