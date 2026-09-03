# eventpulse

Monorepo para la plataforma **eventpulse**: arquitectura de microservicios con
soporte de **RAG sobre pgvector**, mensajería con **Kafka** y despliegue en
**Kubernetes** (Helm).

> Estado: andamiaje inicial. Solo estructura y configuraciones base; la lógica
> de negocio aún no está implementada.

## Servicios

| Servicio                | Lenguaje | Rol                                                            |
|-------------------------|----------|----------------------------------------------------------------|
| `ingestion-gateway`     | Go       | Punto de entrada HTTP que publica eventos crudos en Kafka.     |
| `document-processor`    | Python   | Pipeline LangChain: chunking + embeddings → pgvector.           |
| `metrics-aggregator`   | Go       | Consume eventos y agrega métricas.                             |
| `mcp-server`           | Go       | Expone herramientas RAG sobre MCP consultando pgvector.        |

Paquetes compartidos (Go): `pkg/telemetry` (observabilidad) y `pkg/events`
(temas de Kafka + envelope de eventos).

## Flujo de datos (conceptual)

```
                       ┌──────────────────────┐
   [Fuentes / API] --> │   ingestion-gateway  │
                       └──────────┬───────────┘
                                  │  Kafka: raw.events
                                  ▼
                       ┌──────────────────────┐
                       │  document-processor  │  (LangChain + embeddings)
                       └──────────┬───────────┘
              Kafka: docs.embedded│
                                  ▼
                       ┌──────────────────────┐        ┌─────────────────┐
                       │  Postgres / pgvector │◄───────┤  mcp-server     │
                       └──────────────────────┘  RAG   │  (herramientas  │
                                  ▲                     │   MCP → LLM)    │
                                  │  Kafka: metrics.ticks
                       ┌──────────────────────┐        └─────────────────┘
                       │  metrics-aggregator  │
                       └──────────────────────┘
```

## Estructura

```
eventpulse/
├── go.work                  # workspace Go que enlaza todos los módulos
├── Makefile                 # dev-env, lint, build-all, test, tidy
├── docker-compose.yml       # entorno de desarrollo local
├── services/
│   ├── ingestion-gateway/   # Go
│   ├── document-processor/  # Python (LangChain)
│   ├── metrics-aggregator/  # Go
│   └── mcp-server/          # Go
├── pkg/
│   ├── telemetry/           # Go (compartido)
│   └── events/              # Go (compartido)
└── deploy/
    ├── docker/              # Dockerfiles por servicio
    └── helm/eventpulse/     # Chart de Helm
```

## Arrancar el entorno de desarrollo

Requisitos: Docker + Docker Compose, Go 1.23, Python 3.12.

```bash
# 1. Levanta Kafka + Postgres(pgvector) + los 4 servicios
make dev-env

# 2. Verifica que todo está sano
docker compose ps

# 3. Lint y build de todos los módulos Go
make lint
make build-all

# 4. (Opcional) entorno Python aislado para document-processor
python -m venv services/document-processor/.venv
source services/document-processor/.venv/bin/activate
pip install -r services/document-processor/requirements.txt
```

Para detener y limpiar:

```bash
make dev-env-down
```

### Puntos de acceso a Kafka

El broker publica dos listeners, porque la dirección anunciada debe ser
alcanzable por el cliente que la recibe:

| Desde                        | Bootstrap server  |
|------------------------------|-------------------|
| Otro contenedor de compose   | `kafka:9092`      |
| El host (Go local, CLI)      | `localhost:29092` |

Los servicios de compose ya reciben `KAFKA_BROKERS=kafka:9092`. Si ejecutas un
servicio directamente en el host, apunta al listener externo:

```bash
KAFKA_BROKERS=localhost:29092 go run ./services/ingestion-gateway
```

## Indexación RAG (`document-processor`)

El consumidor lee `raw.events`, se queda con los eventos `document_uploaded`,
parte `payload.content` con `RecursiveCharacterTextSplitter` (500 / 50 por
defecto) y escribe los vectores en la tabla `document_embeddings` de pgvector
(`services/document-processor/schema.sql`), que es la que consulta el
`mcp-server`.

Los ids de cada chunk se derivan del documento, así que una reentrega de Kafka
hace *upsert* en lugar de duplicar; los chunks que sobran de una versión anterior
se borran tras insertar la nueva.

Variables de entorno: ver `services/document-processor/.env.example`. Las claves
mínimas son `KAFKA_BROKERS`, `POSTGRES_URI` (o los `POSTGRES_*` por separado) y
`HUGGINGFACEHUB_API_TOKEN`, ya que Hugging Face es el proveedor por defecto de
embeddings y LLM. Con `EMBEDDINGS_PROVIDER=fake` el pipeline funciona sin API
key ni red, útil para pruebas locales.

```bash
# Levanta el servicio sin necesitar credenciales de embeddings
EMBEDDINGS_PROVIDER=fake docker compose up -d document-processor

# Publica un documento a través del gateway
curl -X POST localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"source":"docs-api","event_type":"document_uploaded",
       "payload":{"document_id":"doc-1","content":"texto largo a indexar..."}}'

# Comprueba los chunks almacenados
docker compose exec postgres psql -U postgres -d eventpulse_db \
  -c 'select document_id, chunk_index from document_embeddings order by 1,2;'
```

Sondas HTTP en el puerto 8000: `/healthz` (liveness) y `/readyz` (incluye los
contadores del consumidor). El topic `docs.embedded` sigue reservado para
notificar aguas abajo; hoy el servicio escribe directamente en pgvector.

## Despliegue en Kubernetes

```bash
helm install eventpulse deploy/helm/eventpulse \
  --namespace eventpulse --create-namespace
```

El chart asume que Kafka y Postgres son alcanzables desde el clúster (ver
`values.yaml`). Ajusta las variables de conexión antes de instalar en producción.

## Convenciones

- Módulos Go bajo `github.com/eventpulse/<nombre>`.
- `go.work` resuelve los paquetes compartidos de forma local; los `go.mod`
  usan `replace` a `../../pkg/...` para builds autónomos.
- Commits y PRs pasan por `make lint` y `make build-all`.
