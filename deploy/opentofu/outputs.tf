output "cluster_name" {
  description = "k3d cluster name."
  value       = var.cluster_name
}

output "kubeconfig" {
  description = "Path to the kubeconfig file."
  value       = var.kubeconfig_path
}

output "namespace" {
  description = "Namespace where eventpulse is deployed."
  value       = var.namespace
}

output "ingress_base_url" {
  description = "Base URL to reach the ingress loadbalancer."
  value       = "localhost:${var.ingress_ports[0]}"
}

output "ingestion_gateway_url" {
  description = "URL to reach the ingestion-gateway healthz through the ingress."
  value       = "http://localhost:${var.ingress_ports[0]}/healthz (Host: events.localhost)"
}

output "mcp_server_url" {
  description = "URL to reach the mcp-server healthz through the ingress."
  value       = "http://localhost:${var.ingress_ports[1]}/healthz (Host: mcp.localhost)"
}

output "kafka_brokers" {
  description = "In-cluster Kafka bootstrap address."
  value       = "kafka:9092"
}

output "postgres" {
  description = "In-cluster Postgres endpoint."
  value       = "postgres:5432/${"eventpulse_db"}"
}