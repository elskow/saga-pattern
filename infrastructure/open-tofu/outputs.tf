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

output "k6_runner_ip" {
  description = "IP address of the K6 load testing node"
  value       = lxd_instance.node["k6-runner"].ipv4_address
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
    zipkin     = "http://${var.nodes["observability-node"].ip_address}:9411"
  }
}

output "saga_urls" {
  description = "URLs for Saga services (when running)"
  value = {
    choreography_order_api  = "http://${var.nodes["saga-node"].ip_address}:8081/api/orders"
    orchestration_order_api = "http://${var.nodes["saga-node"].ip_address}:8081/api/orders"
    kafka                   = "${var.nodes["saga-node"].ip_address}:9092"
    postgres                = "${var.nodes["saga-node"].ip_address}:5432"
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
# Test Commands
#------------------------------------------------------------------------------

output "test_commands" {
  description = "Useful commands for running tests"
  value       = <<-EOT
# Start Choreography pattern:
ssh ubuntu@${var.nodes["saga-node"].ip_address} "cd ~/saga && sudo docker compose -f docker-compose.choreography.yml up -d"

# Start Orchestration pattern:
ssh ubuntu@${var.nodes["saga-node"].ip_address} "cd ~/saga && sudo docker compose -f docker-compose.orchestration.yml up -d"

# Stop services (before switching patterns):
ssh ubuntu@${var.nodes["saga-node"].ip_address} "cd ~/saga && sudo docker compose -f docker-compose.choreography.yml down"
ssh ubuntu@${var.nodes["saga-node"].ip_address} "cd ~/saga && sudo docker compose -f docker-compose.orchestration.yml down"

# Run k6 test from k6-runner:
ssh ubuntu@${var.nodes["k6-runner"].ip_address} "BASE_URL=http://${var.nodes["saga-node"].ip_address}:8081 k6 run -"

# View logs:
ssh ubuntu@${var.nodes["saga-node"].ip_address} "cd ~/saga && sudo docker compose -f docker-compose.choreography.yml logs -f"
EOT
}
