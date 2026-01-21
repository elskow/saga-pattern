#------------------------------------------------------------------------------
# Local variables
#------------------------------------------------------------------------------

locals {
  ssh_public_key = var.ssh_public_key
  k3s_server_ip  = var.nodes["k3s-server"].ip_address
}

#------------------------------------------------------------------------------
# LXD Instances
#------------------------------------------------------------------------------

resource "lxd_instance" "node" {
  for_each = var.nodes

  name    = each.key
  image   = var.instance_image
  type    = var.instance_type
  project = var.lxd_project

  profiles = []

  config = {
    "boot.autostart"        = "true"
    "user.access_interface" = "enp5s0"

    # Cloud-init network configuration
    "cloud-init.network-config" = <<-EOF
      version: 2
      ethernets:
        enp5s0:
          dhcp4: false
          addresses:
            - ${each.value.ip_address}/24
          routes:
            - to: default
              via: 192.168.11.1
          nameservers:
            addresses:
              - 1.1.1.1
              - 8.8.8.8
    EOF

    # Cloud-init user data for SSH key and basic setup
    "cloud-init.user-data" = <<-EOF
      #cloud-config
      hostname: ${each.key}
      manage_etc_hosts: true
      
      users:
        - name: ubuntu
          sudo: ALL=(ALL) NOPASSWD:ALL
          shell: /bin/bash
          ssh_authorized_keys:
            - ${local.ssh_public_key}
      
      package_update: true
      package_upgrade: true
      
      packages:
        - curl
        - wget
        - htop
        - vim
        - git
        - jq
        - ca-certificates
        - apt-transport-https
      
      runcmd:
        - echo "Node ${each.key} initialized"
    EOF
  }

  limits = {
    cpu    = each.value.vcpu
    memory = each.value.memory
  }

  device {
    name = "root"
    type = "disk"
    properties = {
      path = "/"
      pool = var.storage_pool
      size = each.value.storage
    }
  }

  device {
    name = "eth0"
    type = "nic"
    properties = {
      nictype = "bridged"
      parent  = var.network_bridge
      name    = "eth0"
    }
  }

  wait_for_network = true

  timeouts = {
    create = "10m"
    delete = "5m"
  }
}

#------------------------------------------------------------------------------
# K3s Server Setup
#------------------------------------------------------------------------------

resource "null_resource" "k3s_server" {
  depends_on = [lxd_instance.node]

  connection {
    type        = "ssh"
    user        = "ubuntu"
    host        = var.nodes["k3s-server"].ip_address
    private_key = file(var.ssh_private_key_path)
    agent       = false
    timeout     = "5m"
  }

  # Wait for cloud-init to complete
  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "echo 'Cloud-init completed on k3s-server'"
    ]
  }

  # Install K3s server
  provisioner "remote-exec" {
    inline = [
      "curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC='server --disable=traefik --write-kubeconfig-mode=644' sh -",
      "sudo systemctl enable k3s",
      "sleep 10",
      "sudo kubectl wait --for=condition=Ready node --all --timeout=300s || true",
      "echo 'K3s server installed successfully'"
    ]
  }

  # Install Helm
  provisioner "remote-exec" {
    inline = [
      "curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash",
      "echo 'export KUBECONFIG=/etc/rancher/k3s/k3s.yaml' >> ~/.bashrc",
      "helm version",
      "echo 'Helm installed successfully'"
    ]
  }

  # Get K3s token for agents
  provisioner "remote-exec" {
    inline = [
      "sudo cat /var/lib/rancher/k3s/server/node-token > /tmp/k3s-token",
      "sudo chmod 644 /tmp/k3s-token"
    ]
  }
}

# Fetch K3s token from server
data "external" "k3s_token" {
  depends_on = [null_resource.k3s_server]

  program = ["bash", "-c", <<-EOF
    TOKEN=$(ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i ${var.ssh_private_key_path} ubuntu@${var.nodes["k3s-server"].ip_address} 'sudo cat /var/lib/rancher/k3s/server/node-token' 2>/dev/null)
    echo "{\"token\": \"$TOKEN\"}"
  EOF
  ]
}

#------------------------------------------------------------------------------
# K3s Agent Setup
#------------------------------------------------------------------------------

resource "null_resource" "k3s_agent_1" {
  depends_on = [null_resource.k3s_server, data.external.k3s_token]

  connection {
    type        = "ssh"
    user        = "ubuntu"
    host        = var.nodes["k3s-agent-1"].ip_address
    private_key = file(var.ssh_private_key_path)
    agent       = false
    timeout     = "5m"
  }

  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "echo 'Cloud-init completed on k3s-agent-1'"
    ]
  }

  provisioner "remote-exec" {
    inline = [
      "curl -sfL https://get.k3s.io | K3S_URL=https://${local.k3s_server_ip}:6443 K3S_TOKEN=${data.external.k3s_token.result.token} sh -",
      "sudo systemctl enable k3s-agent",
      "echo 'K3s agent installed on k3s-agent-1'"
    ]
  }
}

resource "null_resource" "k3s_agent_2" {
  depends_on = [null_resource.k3s_server, data.external.k3s_token]

  connection {
    type        = "ssh"
    user        = "ubuntu"
    host        = var.nodes["k3s-agent-2"].ip_address
    private_key = file(var.ssh_private_key_path)
    agent       = false
    timeout     = "5m"
  }

  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "echo 'Cloud-init completed on k3s-agent-2'"
    ]
  }

  provisioner "remote-exec" {
    inline = [
      "curl -sfL https://get.k3s.io | K3S_URL=https://${local.k3s_server_ip}:6443 K3S_TOKEN=${data.external.k3s_token.result.token} sh -",
      "sudo systemctl enable k3s-agent",
      "echo 'K3s agent installed on k3s-agent-2'"
    ]
  }
}

#------------------------------------------------------------------------------
# K6 Runner Setup
#------------------------------------------------------------------------------

resource "null_resource" "k6_runner" {
  depends_on = [lxd_instance.node]

  connection {
    type        = "ssh"
    user        = "ubuntu"
    host        = var.nodes["k6-runner"].ip_address
    private_key = file(var.ssh_private_key_path)
    agent       = false
    timeout     = "5m"
  }

  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "echo 'Cloud-init completed on k6-runner'"
    ]
  }

  # Install K6
  provisioner "remote-exec" {
    inline = [
      "sudo gpg -k",
      "sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69",
      "echo 'deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main' | sudo tee /etc/apt/sources.list.d/k6.list",
      "sudo apt-get update",
      "sudo apt-get install -y k6",
      "k6 version",
      "echo 'K6 installed successfully'"
    ]
  }
}

#------------------------------------------------------------------------------
# Verify K3s Cluster
#------------------------------------------------------------------------------

resource "null_resource" "verify_cluster" {
  depends_on = [
    null_resource.k3s_server,
    null_resource.k3s_agent_1,
    null_resource.k3s_agent_2
  ]

  connection {
    type        = "ssh"
    user        = "ubuntu"
    host        = var.nodes["k3s-server"].ip_address
    private_key = file(var.ssh_private_key_path)
    agent       = false
    timeout     = "5m"
  }

  provisioner "remote-exec" {
    inline = [
      "sleep 30",
      "sudo kubectl get nodes -o wide",
      "sudo kubectl get pods -A",
      "echo 'K3s cluster verification complete'"
    ]
  }
}
