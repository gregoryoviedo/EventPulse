output "cluster_name" {
  description = "k3d cluster name (bootstrap_mode k3d only)."
  value       = var.cluster_name
}

output "bootstrap_mode" {
  description = "Cluster bootstrap strategy in use."
  value       = var.bootstrap_mode
}

output "kubeconfig" {
  description = "Path to the kubeconfig file."
  value       = var.kubeconfig_path
}

output "namespace" {
  description = "Namespace where eventpulse is deployed."
  value       = var.namespace
}

output "ingestion_gateway_url" {
  description = "URL to reach the ingestion-gateway healthz through the ingress."
  value       = "http://${var.ingress_host_ingestion}/healthz"
}

output "mcp_server_url" {
  description = "URL to reach the mcp-server healthz through the ingress."
  value       = "http://${var.ingress_host_mcp}/healthz"
}

output "kafka_brokers" {
  description = "In-cluster Kafka bootstrap address."
  value       = "kafka:9092"
}

output "postgres" {
  description = "In-cluster Postgres endpoint."
  value       = "postgres:5432/${"eventpulse_db"}"
}