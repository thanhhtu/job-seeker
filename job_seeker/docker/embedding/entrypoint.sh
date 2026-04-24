#!/bin/bash

# Start the Ollama server in the background
ollama serve &
OLLAMA_PID=$!

# Wait for the server to be ready
echo "Waiting for Ollama server..."
until curl -s http://localhost:11434/api/tags > /dev/null 2>&1; do
    sleep 1
done

# Pull bge-m3 if it is not available
echo "Pulling bge-m3..."
ollama pull bge-m3

echo "bge-m3 ready!"

# Keep the server running
wait $OLLAMA_PID
