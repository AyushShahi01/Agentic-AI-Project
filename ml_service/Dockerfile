# GPU inference service. Needs the NVIDIA container toolkit (Docker Desktop: WSL2 + GPU support).
# torch comes from the base image (CUDA 12.8 runtime); don't reinstall it.
FROM pytorch/pytorch:2.11.0-cuda12.8-cudnn9-runtime

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    ML_WEIGHTS_DIR=/app/weights \
    # Inside the container only; compose publishes the port on 127.0.0.1 of the host.
    ML_HOST=0.0.0.0 ML_PORT=8001

COPY requirements.txt .
RUN grep -v -E '^(pytest|httpx)' requirements.txt > /tmp/req.txt \
    && pip install --no-cache-dir -r /tmp/req.txt

COPY app ./app
# Weights are mounted at /app/weights (see docker-compose.yml, profile "ml").
EXPOSE 8001
CMD ["sh", "-c", "uvicorn app.main:app --host $ML_HOST --port $ML_PORT"]
