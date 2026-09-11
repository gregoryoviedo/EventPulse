variable "cluster_name" {
  description = "Name of the k3d cluster."
  type        = string
  default     = "eventpulse"
}

variable "kubeconfig_path" {
  description = "Path to the kubeconfig used by the kubernetes/helm providers."
  type        = string
  default     = "~/.kube/config"
}

variable "namespace" {
  description = "Kubernetes namespace for the eventpulse release."
  type        = string
  default     = "eventpulse"
}

variable "image_registry" {
  description = "Container image registry used by the chart."
  type        = string
  default     = "ghcr.io/eventpulse"
}

variable "image_tag" {
  description = "Image tag referenced by the chart."
  type        = string
  default     = "0.1.0"
}

variable "embeddings_provider" {
  description = "Embedding provider (fake | huggingface | openai)."
  type        = string
  default     = "fake"
}

variable "huggingface_token" {
  description = "Hugging Face API token for the huggingface provider."
  type        = string
  default     = ""
  sensitive   = true
}

variable "openai_api_key" {
  description = "OpenAI API key for the openai provider."
  type        = string
  default     = ""
  sensitive   = true
}

variable "llm_provider" {
  description = "Generation provider (fake | huggingface | openai | google)."
  type        = string
  default     = "fake"
}

variable "postgres_password" {
  description = "Password of the in-cluster Postgres user."
  type        = string
  default     = "postgres"
  sensitive   = true
}

variable "ingress_ports" {
  description = "Host ports mapped to the k3d loadbalancer for the ingress (events, mcp)."
  type        = list(number)
  default     = [18080, 18090]
}