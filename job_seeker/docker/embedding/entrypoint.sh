#!/bin/bash

ollama serve &
OLLAMA_PID=$!

echo "Waiting for Ollama server..."
until curl -s http://127.0.0.1:11434/api/tags > /dev/null 2>&1; do
    echo "  ...still waiting"
    sleep 2
done

echo "Ollama server ready!"
echo "Pulling bge-m3..."
ollama pull bge-m3
echo "bge-m3 ready!"

wait $OLLAMA_PID
