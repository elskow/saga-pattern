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

output "k3s_server_ip" {
  description = "IP address of the K3s control plane node"
  value       = lxd_instance.node["k3s-server"].ipv4_address
}

output "k3s_agent_ips" {
  description = "IP addresses of the K3s agent nodes"
  value = [
    lxd_instance.node["k3s-agent-1"].ipv4_address,
    lxd_instance.node["k3s-agent-2"].ipv4_address,
  ]
}

output "k6_runner_ip" {
  description = "IP address of the K6 load testing node"
  value       = lxd_instance.node["k6-runner"].ipv4_address
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
# Ansible Inventory Output
#------------------------------------------------------------------------------

output "ansible_inventory" {
  description = "Ansible inventory in INI format"
  value       = <<-EOT
[k3s_server]
${lxd_instance.node["k3s-server"].ipv4_address} ansible_user=ubuntu

[k3s_agents]
${lxd_instance.node["k3s-agent-1"].ipv4_address} ansible_user=ubuntu
${lxd_instance.node["k3s-agent-2"].ipv4_address} ansible_user=ubuntu

[k6_runners]
${lxd_instance.node["k6-runner"].ipv4_address} ansible_user=ubuntu

[k3s_cluster:children]
k3s_server
k3s_agents

[all:vars]
ansible_python_interpreter=/usr/bin/python3
EOT
}

#------------------------------------------------------------------------------
# K3s Cluster Outputs
#------------------------------------------------------------------------------

output "k3s_kubeconfig_command" {
  description = "Command to fetch kubeconfig from K3s server"
  value       = "ssh ubuntu@${lxd_instance.node["k3s-server"].ipv4_address} 'sudo cat /etc/rancher/k3s/k3s.yaml' | sed 's/127.0.0.1/${lxd_instance.node["k3s-server"].ipv4_address}/g' > ~/.kube/k3s-config"
}

output "k3s_token" {
  description = "K3s join token (sensitive)"
  value       = try(data.external.k3s_token.result.token, "Token not yet available - run apply first")
  sensitive   = true
}
