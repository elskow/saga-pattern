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

output "gatling_runner_ip" {
  description = "IP address of the Gatling load testing node"
  value       = lxd_instance.node["gatling-runner"].ipv4_address
}

output "observability_node_ip" {
  description = "IP address of the observability node"
  value       = lxd_instance.node["observability-node"].ipv4_address
}

output "observability_urls" {
  description = "URLs for observability services"
  value = {
    prometheus = "http://${var.nodes["observability-node"].ip_address}:9090"
    grafana    = "http://${var.nodes["observability-node"].ip_address}:3000"
    jaeger     = "http://${var.nodes["saga-node"].ip_address}:16686"
  }
}

output "saga_urls" {
  description = "URLs for Saga services (when running)"
  value = {
    choreography_order_api  = "http://${var.nodes["saga-node"].ip_address}:8081/api/orders"
    orchestration_order_api = "http://${var.nodes["saga-node"].ip_address}:8085/api/orders"
    kafka                   = "${var.nodes["saga-node"].ip_address}:9092"
    jaeger_ui               = "http://${var.nodes["saga-node"].ip_address}:16686"
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
    GATLING_RUNNER_IP   = var.nodes["gatling-runner"].ip_address
    OBSERVABILITY_IP    = var.nodes["observability-node"].ip_address
    CHOREOGRAPHY_PORT   = "8081"
    ORCHESTRATION_PORT  = "8085"
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
# GATLING BENCHMARKS (on gatling-runner)
# =============================================================================

# Run choreography benchmark:
ssh ubuntu@${var.nodes["gatling-runner"].ip_address} "~/run-benchmark.sh choreography thesis-baseline SustainedMixedSimulation ${var.nodes["saga-node"].ip_address}"

# Run orchestration benchmark:
ssh ubuntu@${var.nodes["gatling-runner"].ip_address} "~/run-benchmark.sh orchestration thesis-baseline SustainedMixedSimulation ${var.nodes["saga-node"].ip_address}"

# Quick warmup:
ssh ubuntu@${var.nodes["gatling-runner"].ip_address} "~/warmup.sh ${var.nodes["saga-node"].ip_address} 8081 100"

# Copy results to local:
scp -r ubuntu@${var.nodes["gatling-runner"].ip_address}:~/results/* ./results/

# =============================================================================
# OBSERVABILITY
# =============================================================================

# Prometheus: http://${var.nodes["observability-node"].ip_address}:9090
# Grafana:    http://${var.nodes["observability-node"].ip_address}:3000 (admin/admin)
# Jaeger:     http://${var.nodes["saga-node"].ip_address}:16686
EOT
}
