from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from FlagEmbedding import FlagReranker
from typing import List

app = FastAPI()

model = FlagReranker('BAAI/bge-reranker-v2-m3', use_fp16=True)

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
        if isinstance(scores, float):
            scores = [scores]
        return {"scores": scores}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    