# Bootstrap the Kubernetes cluster and prepare the container images.
resource "null_resource" "bootstrap_k3d" {
  triggers = {
    cluster  = var.cluster_name
    mode     = var.bootstrap_mode
    registry = var.image_registry
    tag      = var.image_tag
    ports    = join(",", [for p in var.ingress_ports : tostring(p)])
  }

  provisioner "local-exec" {
    command = "${path.module}/bootstrap.sh"
    environment = {
      K3D_CLUSTER    = var.cluster_name
      BOOTSTRAP_MODE = var.bootstrap_mode
      IMAGE_REGISTRY = var.image_registry
      IMAGE_TAG      = var.image_tag
      INGRESS_PORTS  = join(",", [for p in var.ingress_ports : tostring(p)])
    }
  }
}

# Namespace and release are owned by Helm (create_namespace handles the
# namespace; the chart also declares it idempotently).

# Helm release of the eventpulse chart (services + in-cluster kafka/postgres).
resource "helm_release" "eventpulse" {
  depends_on = [null_resource.bootstrap_k3d]

  name             = "eventpulse"
  chart            = "${path.module}/../helm/eventpulse"
  namespace        = var.namespace
  create_namespace = true
  wait             = true
  timeout          = 600

  set {
    name  = "image.registry"
    value = var.image_registry
  }
  set {
    name  = "image.tag"
    value = var.image_tag
  }
  set {
    name  = "image.registryUsername"
    value = var.registry_username
  }
  set {
    name  = "image.registryPassword"
    value = var.registry_password
  }
  set_list {
    name  = "image.pullSecrets"
    value = var.pull_secrets
  }
  set {
    name  = "ingress.hosts.ingestionGateway"
    value = var.ingress_host_ingestion
  }
  set {
    name  = "ingress.hosts.mcpServer"
    value = var.ingress_host_mcp
  }
  set {
    name  = "postgres.password"
    value = var.postgres_password
  }
  set {
    name  = "embeddings.provider"
    value = var.embeddings_provider
  }
  set {
    name  = "embeddings.huggingfaceToken"
    value = var.huggingface_token
  }
  set {
    name  = "embeddings.openaiApiKey"
    value = var.openai_api_key
  }
  set {
    name  = "llm.provider"
    value = var.llm_provider
  }
}