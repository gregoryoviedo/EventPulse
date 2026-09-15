# Eventpulse — Guía de instalación y despliegue

Guía para levantar el sistema de microservicios **eventpulse** de cero, tanto en
desarrollo local como en el homelab (k3s). Cubre los requisitos, las variables
de entorno, los comandos del `Makefile` y los pasos de despliegue.

> Reconstrucción desde cero: si esta máquina se limpió, seguir esta guía de
> arriba a abajo vuelve a dejar todo operativo.

---

## 1. Arquitectura

| Servicio             | Lenguaje | Rol                                                             |
|----------------------|----------|-----------------------------------------------------------------|
| `ingestion-gateway`  | Go       | HTTP (8080) que publica eventos crudos en Kafka (`raw.events`). |
| `document-processor` | Python   | Pipeline LangChain: chunking + embeddings → Postgres/pgvector.  |
| `metrics-aggregator` | Go       | Consume `raw.events` y agrega métricas.                         |
| `mcp-server`         | Go       | Herramientas MCP (RAG sobre pgvector) consultando Postgres.     |

Infraestructura:

- **Kafka** (single-broker KRaft) — mensajería entre servicios.
- **Postgres 16 + pgvector** — embeddings (tabla `document_embeddings`).

Flujo de datos:

```
[Fuentes/API] → ingestion-gateway → Kafka(raw.events) → document-processor
                                                        → Postgres/pgvector
                                                        → mcp-server (RAG → LLM)
                                                → metrics-aggregator (métricas)
```

---

## 2. Requisitos

### Desarrollo local (macOS / esta máquina)

| Herramienta | Versión | Instalación (macOS)                          |
|-------------|---------|----------------------------------------------|
| Docker + Compose | — | OrbStack (`brew install --cask orbstack`) o Docker Desktop |
| Go | 1.25.x | `brew install go`                            |
| Python | 3.12 | `brew install python@3.12`                   |
| OpenTofu | ≥1.8 | `brew install opentofu`                      |
| Helm | ≥3.14 | `brew install helm`                          |
| k3d (opcional, K8s local) | ≥5 | `brew install k3d`                           |
| golangci-lint (opcional) | ≥1.61 | `brew install golangci-lint`                 |

> El despliegue local por defecto solo necesita **Docker + Compose** (comando
> `make dev-env`). OpenTofu/Helm/k3d son solo para el modo Kubernetes.

### Homelab (Linux, k3s)

- Linux con **k3s** (el bootstrap de OpenTofu lo instala si `bootstrap_mode = "k3s"`).
- **Docker** para `make images-build` (puede ser el host de desarrollo).
- Cuenta de GitHub para publicar imágenes en **GHCR** (usa el PAT de cada uno).
- OpenTofu (en la máquina desde la que se ejecuta `tofu apply`).

---

## 3. Variables de entorno

Se pasan por el shell o un archivo `.env` en la raíz del repo (docker-compose
las lee automáticamente). En Kubernetes se configuran en `terraform.tfvars`.

| Variable | Default | Descripción |
|----------|---------|-------------|
| `EMBEDDINGS_PROVIDER` | `huggingface` | `fake` \| `huggingface` \| `openai`. `fake` corre sin tokens. |
| `HUGGINGFACEHUB_API_TOKEN` | — | Token de HuggingFace (si provider = huggingface). |
| `OPENAI_API_KEY` | — | Key de OpenAI (si provider = openai). |
| `EMBEDDING_MODEL` | `BAAI/bge-large-en-v1.5` | Modelo de embeddings. **Debe coincidir** entre document-processor y mcp-server. |
| `EMBEDDING_DIMENSIONS` | `1024` | Debe coincidir con el ancho `vector(n)` de la tabla. |
| `LLM_PROVIDER` | `huggingface` | `fake` \| `huggingface` \| `openai` \| `google`. |
| `LLM_MODEL` | `Qwen/Qwen2.5-72B-Instruct` | Modelo de generación. |
| `OPENAI_API_BASE` | — | Override de base URL (openai). |
| `GOOGLE_API_KEY` | — | Key de Google (si LLM provider = google). |

Ejemplo mínimo para probar sin servicios externos:

```bash
export EMBEDDINGS_PROVIDER=fake
export LLM_PROVIDER=fake
```

---

## 4. Puertos (dev local)

| Puerto | Servicio                          |
|--------|-----------------------------------|
| 29092  | Kafka (listener EXTERNAL, host)   |
| 5432   | Postgres                          |
| 8080   | ingestion-gateway                 |
| 8000   | document-processor                |
| 9090   | metrics-aggregator                |
| 8090   | mcp-server                        |

---

## 5. Entorno de desarrollo local (docker-compose)

```bash
cd ~/Dev/Personal/eventpulse

# 1. Levantar Kafka + Postgres + los 4 servicios
make dev-env                       # = docker compose up -d

# 2. Verificar estado
docker compose ps
docker compose logs -f ingestion-gateway

# 3. Healthchecks
curl localhost:8080/healthz
curl localhost:8000/healthz
curl localhost:9090/healthz
curl localhost:8090/healthz
```

Teardown (elimina contenedores, red y el volumen `pgdata`):

```bash
make dev-env-down                  # = docker compose down -v
```

> Los datos de Postgres persisten en el volumen `eventpulse_pgdata` hasta el
> `down -v`.

### Lint / build / test

```bash
make lint          # golangci-lint por módulo + flake8 (si está)
make build-all     # go build de todos los módulos + docker compose build
make test          # go test de todos los módulos
make tidy          # go mod tidy + go work sync
```

---

## 6. Kubernetes local (k3d, opcional)

```bash
make k8s-dev-env        # crea clúster k3d + tofu apply
make k8s-verify         # smoke test por el ingress (hosts events.localhost / mcp.localhost)
make k8s-dev-env-down   # tofu destroy + k3d cluster delete
```

---

## 7. Despliegue en el homelab (k3s + GHCR)

### 7.1 Publicar las imágenes (una vez, o desde CI)

```bash
cd ~/Dev/Personal/eventpulse
export REGISTRY=ghcr.io/gregoryoviedo

docker login ghcr.io            # PAT con write:packages
make images-push                # build + push de los 4 servicios (tag 0.1.0)
```

### 7.2 Preparar `terraform.tfvars`

```bash
cd deploy/opentofu
cp terraform.tfvars.example terraform.tfvars
```

Campos clave (secrets nunca se commitean):

```hcl
bootstrap_mode     = "k3s"                      # instala k3s si falta
kubeconfig_path    = "/etc/rancher/k3s/k3s.yaml"
namespace          = "eventpulse"

image_registry     = "ghcr.io/gregoryoviedo"
image_tag          = "0.1.0"
registry_username  = "gregoryoviedo"
registry_password  = "ghp_xxx"                  # PAT con read:packages (si es privado)

ingress_host_ingestion = "events.homelab.local"
ingress_host_mcp       = "mcp.homelab.local"

embeddings_provider = "huggingface"
huggingface_token   = "hf_xxx"                  # para RAG real
# embeddings_provider = "fake"                  # para probar sin token

llm_provider        = "huggingface"

postgres_password   = "change-me"
```

### 7.3 Desplegar

```bash
tofu init
tofu apply          # instala k3s si falta, crea el release Helm y los PVCs
```

### 7.4 Verificar

```bash
make k8s-verify     # healthz + publica un evento de prueba
```

- Resolver `events.homelab.local` y `mcp.homelab.local` al IP del homelab
  (DNS o `/etc/hosts`). El ingress de k3s (Traefik) escucha en `:80`/`:443`.

### 7.5 Teardown

```bash
make k8s-dev-env-down    # = tofu destroy + (k3d cluster delete, no aplica a k3s)
```

---

## 8. Persistencia y backups

- **Dev local**: los datos de Postgres viven en el volumen `eventpulse_pgdata`.
- **Homelab**: Kafka y Postgres usan **PVCs** (`storageSize: 2Gi`) que persisten
  entre `tofu destroy` (no se borran).
- Haz backup del PVC de Postgres (tabla `document_embeddings`) si quieres
  conservar el RAG indexado.

---

## 9. Troubleshooting

- **Los embeddings no alinean en RAG**: `document-processor` y `mcp-server`
  deben usar el mismo `EMBEDDING_MODEL` y `EMBEDDING_DIMENSIONS`.
- **Quiero probar sin tokens externos**: `EMBEDDINGS_PROVIDER=fake` y
  `LLM_PROVIDER=fake`.
- **Kafka no arranca**: comprobar `docker compose ps`; el broker single-node
  requiere los replication factors a 1 (ya configurados en el compose).
- **Ingress no resuelve**: verificar que los hostnames apuntan al IP del
  homelab y que Traefik está escuchando en `:80`/`:443`.
- **Imágenes privadas en GHCR**: asegurar `registry_password` (PAT con
  `read:packages`); el chart crea el secret `regcred` y lo adjunta a los pods.
```