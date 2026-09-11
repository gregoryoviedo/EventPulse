# eventpulse

Monorepo para la plataforma **eventpulse**: arquitectura de microservicios con
soporte de **RAG sobre pgvector**, mensajería con **Kafka** y despliegue en
**Kubernetes** (k3d + Helm) aprovisionado con **OpenTofu**.

> Estado: servicios funcionales (ingesta, indexación RAG, agregación de métricas
> y servidor MCP). La telemetría se exporta por OTLP cuando hay collector.

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
├── Makefile                 # dev-env, lint, build-all, test, tidy, k8s-*
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
    ├── helm/eventpulse/     # Chart de Helm (K8s)
    └── opentofu/            # IaC: cluster k3d + despliegue Helm
```

## Arrancar el entorno de desarrollo

Requisitos: Docker + Docker Compose, Go 1.25, Python 3.12.

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

## Servidor MCP (`mcp-server`)

Expone la búsqueda RAG como un servidor **Model Context Protocol** sobre
**Streamable HTTP** (`mark3labs/mcp-go`): consulta `document_embeddings`
directamente en pgvector, sin pasar por el `document-processor`.

La herramienta es:

| Tool        | Descripción                                                        |
|-------------|--------------------------------------------------------------------|
| `rag_search`| `query` (obligatoria) + `top_k` (1–10, default 3) → chunks con score y metadata |

Para que los vectores de la consulta alineen con los indexados, `EMBEDDINGS_PROVIDER`
y `EMBEDDING_MODEL` deben coincidir con los que usó el `document-processor`
(`EMBEDDINGS_PROVIDER=fake` solo prueba el cableado MCP; no rankea datos
indexados con el fake de Python). Puedes reutilizar las credenciales del
`.env` del `document-processor` (`HUGGINGFACEHUB_API_TOKEN`, `EMBEDDING_MODEL`,
`EMBEDDING_DIMENSIONS`) exportándolas en el shell antes de arrancar:

```bash
set -a; source services/document-processor/.env; set +a
EMBEDDINGS_PROVIDER=huggingface docker compose up -d mcp-server
```

El cliente de embeddings de Hugging Face del `mcp-server` habla con el mismo
endpoint de feature-extraction (`router.huggingface.co/hf-inference/models/...`)
que `HuggingFaceEndpointEmbeddings` del procesador, así que las búsquedas
quedan alineadas con lo indexado.

```bash
# Levanta el servicio sin necesitar credenciales de embeddings
EMBEDDINGS_PROVIDER=fake docker compose up -d mcp-server

# Health y readiness
curl localhost:8090/healthz
curl localhost:8090/readyz

# Inicializa una sesión MCP sobre Streamable HTTP
curl -X POST localhost:8090/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'

# Lista las herramientas
curl -X POST localhost:8090/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
```

Configuración: ver `services/mcp-server/.env.example`. Las claves mínimas son
`POSTGRES_URI` (o `POSTGRES_*`), `EMBEDDINGS_PROVIDER` y el token/llave del
provider elegido. El `mcp-server` no consume Kafka.

## Métricas (`metrics-aggregator`)

Además de exponer las métricas Prometheus en `GET /metrics`, el agregador
publica un *snapshot* acumulado de los contadores en el tópico `metrics.ticks`
cada `KAFKA_METRICS_TICK_INTERVAL` (default `15s`), con un evento por
combinación `source`/`event_type`:

```json
{"id":"...","type":"metrics_tick","source":"docs-api","timestamp":1710000000000,
 "payload":{"event_type":"document_uploaded","count":42}}
```

Aún no hay consumidor de `metrics.ticks`: el tópico queda reservado para el
consumidor que lo persista aguas abajo, igual que `docs.embedded`.

## Despliegue en Kubernetes (k3d + OpenTofu)

La plataforma se despliega en un clúster local de **k3s** vía **k3d** (k3s dentro
de Docker, no requiere instalación de servicios en el host) y se aprovisiona con
**OpenTofu** (compatible con Terraform). El chart de Helm incluye los servicios y
las dependencias (Kafka single-broker KRaft y Postgres/pgvector) **dentro del
clúster**, así que es autocontenido.

Requisitos: `brew install k3d helm tofu`.

```bash
# 1. (Opcional) build de imágenes locales usadas por compose
make build-all

# 2. Despliega todo: crea el clúster k3d, importa las imágenes y
#    aplica el chart vía OpenTofu
make k8s-dev-env

# 3. Verificación rápida
make k8s-verify
```

`make k8s-dev-env` equivale a `make k8s-cluster-up && make tofu-apply`. El
`null_resource` de OpenTofu hace el bootstrap del clúster idempotente (crea el
clúster si no existe e importa las imágenes), y el provider `helm` despliega el
chart. El `kafka-init` Job crea los topics `raw.events` y `raw.events.dlq`, y los
Deployments esperan a Kafka/Postgres con `initContainers`.

### Acceso

El Ingress (Traefik incluido en k3d) expone dos hosts; el loadbalancer de k3d
mapea `18080` y `18090` a `:80`:

| Recurso            | URL                                                        |
|--------------------|------------------------------------------------------------|
| ingestion-gateway  | `curl -H "Host: events.localhost" localhost:18080/healthz` |
| mcp-server (MCP)   | `curl -H "Host: mcp.localhost" localhost:18090/healthz`    |
| metrics            | `kubectl port-forward svc/metrics-aggregator 9090:9090 -n eventpulse` |

### OpenTofu

```bash
cd deploy/opentofu
cp terraform.tfvars.example terraform.tfvars   # ajusta secretos (nunca se commitean)
tofu init
tofu plan
tofu apply
tofu destroy        # desinstala la release; el clúster queda a elección
```

Los secretos (token de Hugging Face, API keys, password de Postgres) se pasan por
`terraform.tfvars` / variables de entorno, nunca hardcodeados. Para usar
embeddings reales, setea `embeddings_provider = "huggingface"` y tu token.

### Tear down

```bash
make k8s-dev-env-down     # tofu destroy + k3d cluster delete
```

### Solo Helm (sin OpenTofu)

```bash
helm install eventpulse deploy/helm/eventpulse \
  --namespace eventpulse --create-namespace
```

## Convenciones

- Módulos Go bajo `github.com/eventpulse/<nombre>`.
- `go.work` resuelve los paquetes compartidos de forma local; los `go.mod`
  usan `replace` a `../../pkg/...` para builds autónomos.
- Commits y PRs pasan por `make lint` y `make build-all`.
