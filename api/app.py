from fastapi import FastAPI

from . import event_api, recommendation_api

app = FastAPI(title="Unified API", version="0.1.0")

app.include_router(event_api.router)
app.include_router(recommendation_api.router)

app.add_event_handler("startup", event_api.startup_event)
app.add_event_handler("shutdown", event_api.shutdown_event)

@app.get("/health")
async def health():
    return {"status": "ok"}
