"""document-processor scaffold.

Consumes raw events from Kafka, runs a LangChain pipeline to chunk and embed
documents, and writes the resulting vectors into Postgres (pgvector).

Business logic is intentionally deferred; this is the scaffold.
"""

from fastapi import FastAPI

app = FastAPI(title="eventpulse-document-processor")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


def main() -> None:
    # TODO: wire Kafka consumer + LangChain embedding pipeline + pgvector store.
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
