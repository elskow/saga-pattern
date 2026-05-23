#------------------------------------------------------------------------------
# Instance Outputs
#------------------------------------------------------------------------------

output "instances" {
  description = "Map of all provisioned LXD instances"
  value = {
    for name, instance in lxd_instance.node : name => {
      name         = instance.name
      type         = instance.type
      status       = instance.status
      ipv4_address = instance.ipv4_address
      ipv6_address = instance.ipv6_address
      mac_address  = instance.mac_address
      vcpu         = var.nodes[name].vcpu
      memory       = var.nodes[name].memory
      storage      = var.nodes[name].storage
    }
  }
}

output "instance_ips" {
  description = "Simple map of instance names to their IPv4 addresses"
  value = {
    for name, instance in lxd_instance.node : name => instance.ipv4_address
  }
}

output "saga_node_ip" {
  description = "IP address of the Saga application node"
  value       = lxd_instance.node["saga-node"].ipv4_address
}

output "observability_node_ip" {
  description = "IP address of the observability node"
  value       = lxd_instance.node["observability-node"].ipv4_address
}

output "observability_urls" {
  description = "URLs for SigNoz observability services"
  value = {
    signoz_ui        = "http://${var.nodes["observability-node"].ip_address}:8080"
    otlp_http        = "http://${var.nodes["observability-node"].ip_address}:4318"
    otlp_grpc        = "${var.nodes["observability-node"].ip_address}:4317"
    collector_health = "http://${var.nodes["observability-node"].ip_address}:13133"
  }
}

output "saga_urls" {
  description = "URLs for Saga services (when running)"
  value = {
    choreography_order_api  = "http://${var.nodes["saga-node"].ip_address}:8081/api/orders"
    orchestration_order_api = "http://${var.nodes["saga-node"].ip_address}:8091/api/orders"
    kafka                   = "${var.nodes["saga-node"].ip_address}:9092"
  }
}

#------------------------------------------------------------------------------
# SSH Config Output (for easy access)
#------------------------------------------------------------------------------

output "ssh_config" {
  description = "SSH config entries for all nodes"
  value = join("\n\n", [
    for name, instance in lxd_instance.node : <<-EOT
    Host ${name}
        HostName ${instance.ipv4_address}
        User ubuntu
        StrictHostKeyChecking no
        UserKnownHostsFile /dev/null
    EOT
  ])
}

#------------------------------------------------------------------------------
# Jenkins Integration Outputs
#------------------------------------------------------------------------------

output "jenkins_env" {
  description = "Environment variables for Jenkins pipeline"
  value = {
    SAGA_NODE_IP        = var.nodes["saga-node"].ip_address
    OBSERVABILITY_IP    = var.nodes["observability-node"].ip_address
    CHOREOGRAPHY_PORT   = "8081"
    ORCHESTRATION_PORT  = "8091"
  }
}

#------------------------------------------------------------------------------
# Test Commands
#------------------------------------------------------------------------------

output "test_commands" {
  description = "Useful commands for running tests"
  value       = <<-EOT
# =============================================================================
# SERVICE MANAGEMENT (on saga-node)
# =============================================================================

# Start Choreography pattern:
ssh ubuntu@${var.nodes["saga-node"].ip_address} "cd ~/saga && sudo docker compose -f docker-compose.choreography.yml up -d"

# Start Orchestration pattern:
ssh ubuntu@${var.nodes["saga-node"].ip_address} "cd ~/saga && sudo docker compose -f docker-compose.orchestration.yml up -d"

# Stop services (before switching patterns):
ssh ubuntu@${var.nodes["saga-node"].ip_address} "cd ~/saga && sudo docker compose -f docker-compose.choreography.yml down"
ssh ubuntu@${var.nodes["saga-node"].ip_address} "cd ~/saga && sudo docker compose -f docker-compose.orchestration.yml down"

# View logs:
ssh ubuntu@${var.nodes["saga-node"].ip_address} "cd ~/saga && sudo docker compose -f docker-compose.choreography.yml logs -f"

# =============================================================================
# K6 BENCHMARKS (on saga-node)
# =============================================================================

# Run choreography benchmark:
ssh ubuntu@${var.nodes["saga-node"].ip_address} "cd ~/workspace/saga-pattern && load-testing/thesis/run-k6-thesis.sh --pattern choreography --scenario happy-path --profile thesis-baseline"

# Run orchestration benchmark:
ssh ubuntu@${var.nodes["saga-node"].ip_address} "cd ~/workspace/saga-pattern && load-testing/thesis/run-k6-thesis.sh --pattern orchestration --scenario happy-path --profile thesis-baseline"

# Copy results to local:
scp -r ubuntu@${var.nodes["saga-node"].ip_address}:~/workspace/saga-pattern/results/k6-thesis/* ./results/

# =============================================================================
# OBSERVABILITY
# =============================================================================

# SigNoz UI:        http://${var.nodes["observability-node"].ip_address}:8080
# OTLP HTTP:        http://${var.nodes["observability-node"].ip_address}:4318
# OTLP gRPC:        ${var.nodes["observability-node"].ip_address}:4317
# Collector health: http://${var.nodes["observability-node"].ip_address}:13133
EOT
}
