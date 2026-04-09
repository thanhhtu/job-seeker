-- Cài cả pgvector lẫn pgvectorscale cùng lúc
CREATE EXTENSION IF NOT EXISTS vectorscale CASCADE;

-- Hoặc chỉ cần pgvector thuần
CREATE EXTENSION IF NOT EXISTS vector;
