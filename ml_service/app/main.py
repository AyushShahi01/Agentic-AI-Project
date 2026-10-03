"""ML classification service.

    uvicorn app.main:app --host 127.0.0.1 --port 8001

GET  /v1/health    -> {ready, model_version, device, vram_used_mb}
POST /v1/classify  -> requires X-ML-Token; body <= ML_MAX_BODY_BYTES (256KB); top_k <= 9
"""

import hmac
import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import ValidationError

from app.config import get_settings
from app.model import Predictor, PredictorProtocol
from app.schemas import ClassifyRequest, ClassifyResponse, HealthResponse

logger = logging.getLogger(__name__)


def create_app(predictor: PredictorProtocol | None = None) -> FastAPI:
    settings = get_settings()
    lock = threading.Lock()  # one GPU, one batch at a time

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.predictor is None:
            app.state.predictor = Predictor(
                settings.weights_dir, settings.weights_version, settings.device
            )
        if not settings.token:
            logger.warning("ML_SERVICE_TOKEN is not set; every /v1/classify call will be rejected")
        yield

    app = FastAPI(title="Failure classifier", version="1.0.0", lifespan=lifespan)
    app.state.predictor = predictor

    @app.get("/v1/health", response_model=HealthResponse)
    def health(request: Request) -> HealthResponse:
        p: PredictorProtocol = request.app.state.predictor
        return HealthResponse(
            ready=p.ready, model_version=p.model_version, device=p.device,
            vram_used_mb=p.vram_used_mb(),
        )

    @app.post("/v1/classify", response_model=ClassifyResponse)
    async def classify(
        request: Request, x_ml_token: str | None = Header(default=None)
    ) -> ClassifyResponse:
        if not settings.token or not x_ml_token or not hmac.compare_digest(
            x_ml_token.encode(), settings.token.encode()
        ):
            raise HTTPException(status_code=401, detail="invalid or missing X-ML-Token")
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > settings.max_body_bytes:
            raise HTTPException(status_code=413, detail="request body too large")
        body = await request.body()
        if len(body) > settings.max_body_bytes:
            raise HTTPException(status_code=413, detail="request body too large")
        try:
            payload = ClassifyRequest.model_validate_json(body)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=exc.errors(include_url=False)) from exc
        p: PredictorProtocol = request.app.state.predictor
        if not p.ready:
            raise HTTPException(status_code=503, detail="model not ready")

        def run() -> dict:
            with lock:
                return p.predict(payload.logs, payload.top_k)

        return ClassifyResponse(**await run_in_threadpool(run))

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    s = get_settings()
    uvicorn.run("app.main:app", host=s.host, port=s.port)
