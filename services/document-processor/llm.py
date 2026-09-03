"""RAG completion: retrieval-grounded answer generation.

This is the generation side of the RAG flow. It owns the answer prompt and the
LLM wiring; retrieval stays in store.py and the request handling in main.py.
The chain is the strict composition `prompt | llm` that the endpoint invokes.
"""

from __future__ import annotations

from typing import Sequence

from langchain_core.documents import Document
from langchain_core.language_models import BaseChatModel
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable

from config import ConfigError, Settings

# Strict instruction: the answer must come from the provided context alone.
SYSTEM_PROMPT = (
    "Eres un asistente especializado en análisis de documentos. "
    "Responde a la pregunta del usuario utilizando únicamente el siguiente "
    "contexto provisto. Si la respuesta no se encuentra en el contexto, indica "
    "explícitamente que no posees información suficiente para responder. "
    "No inventes ni asumas datos."
)

RAG_PROMPT = ChatPromptTemplate.from_messages(
    [
        ("system", SYSTEM_PROMPT),
        (
            "human",
            "Contexto:\n{context}\n\n"
            "Pregunta del usuario: {question}\n\n"
            "Respuesta:",
        ),
    ]
)


def build_rag_chain(settings: Settings) -> Runnable:
    """Wire the RAG prompt to the configured chat model (`prompt | llm`)."""
    return RAG_PROMPT | build_llm(settings)


def build_llm(settings: Settings) -> BaseChatModel:
    """Instantiate the generation model described by the environment.

    * openai: ChatOpenAI pointed at OPENAI_API_BASE (falling back to the legacy
      OPENAI_BASE_URL), the same OpenAI-compatible gateway the embeddings use.
    * huggingface: ChatHuggingFace over a serverless HuggingFaceEndpoint.
    * google: ChatGoogleGenerativeAI. The package is not part of the pinned
      requirements (no release is compatible with the langchain-core 0.3 line),
      so it is imported lazily and its absence is reported as a config error.
    * fake: offline stand-in so /api/v1/generate works without an API key.
    """
    if settings.llm_provider == "openai":
        from langchain_openai import ChatOpenAI

        kwargs: dict[str, object] = {
            "model_name": settings.llm_model,
            "openai_api_key": settings.openai_api_key,
            "temperature": 0,
        }
        if settings.openai_base_url:
            kwargs["openai_api_base"] = settings.openai_base_url

        return ChatOpenAI(**kwargs)  # type: ignore[arg-type]

    if settings.llm_provider == "huggingface":
        if not settings.huggingfacehub_api_token:
            raise ConfigError(
                "HUGGINGFACEHUB_API_TOKEN is required when LLM_PROVIDER=huggingface; "
                "set LLM_PROVIDER=fake to run without a generation endpoint"
            )

        from langchain_huggingface import ChatHuggingFace, HuggingFaceEndpoint

        endpoint = HuggingFaceEndpoint(
            repo_id=settings.llm_model,
            huggingfacehub_api_token=settings.huggingfacehub_api_token,
            task="text-generation",
        )

        return ChatHuggingFace(llm=endpoint, temperature=0)

    if settings.llm_provider == "google":
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
        except ImportError as exc:
            raise ConfigError(
                "LLM_PROVIDER=google needs the langchain-google-genai package, "
                "which is not compatible with the pinned langchain-core 0.3 line; "
                "pip install langchain-google-genai or set LLM_PROVIDER=openai"
            ) from exc

        return ChatGoogleGenerativeAI(
            model=settings.llm_model,
            google_api_key=settings.google_api_key,
            temperature=0,
        )

    if settings.llm_provider == "fake":
        return FakeMessagesListChatModel(
            responses=[
                AIMessage(
                    content="Respuesta generada sin un modelo real (LLM_PROVIDER=fake)."
                )
            ]
        )

    raise ConfigError(f"unsupported LLM_PROVIDER {settings.llm_provider!r}")


def format_context(hits: Sequence[tuple[Document, float]]) -> str:
    """Flatten retrieved chunks into the context block handed to the model.

    Each chunk is prefixed with its metadata (title and id) so the answer can
    be traced back to the passage it came from.
    """
    return "\n\n".join(
        f"[Fuente: {_source_title(document)} | Chunk: {document.id}]\n"
        f"{document.page_content}"
        for document, _score in hits
    )


def format_sources(hits: Sequence[tuple[Document, float]]) -> list[dict[str, object]]:
    """Project retrieved chunks onto the sources the API exposes."""
    return [
        {
            "chunk_id": document.id,
            "title": _source_title(document),
            "score": score,
        }
        for document, score in hits
    ]


def _source_title(document: Document) -> str:
    """Pick the most readable label for a chunk, in priority order."""
    for key in ("title", "source", "document_id"):
        value = document.metadata.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()

    return document.id
