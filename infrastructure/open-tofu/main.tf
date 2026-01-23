#------------------------------------------------------------------------------
# Local variables
#------------------------------------------------------------------------------

locals {
  ssh_public_key = var.ssh_public_key
  saga_node_ip   = var.nodes["saga-node"].ip_address
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
# Saga Node Setup (Docker + Docker Compose)
#------------------------------------------------------------------------------

resource "null_resource" "saga_node" {
  depends_on = [lxd_instance.node]

  connection {
    type        = "ssh"
    user        = "ubuntu"
    host        = var.nodes["saga-node"].ip_address
    private_key = file(var.ssh_private_key_path)
    agent       = false
    timeout     = "10m"
  }

  # Wait for cloud-init to complete
  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "echo 'Cloud-init completed on saga-node'"
    ]
  }

  # Install Docker
  provisioner "remote-exec" {
    inline = [
      "curl -fsSL https://get.docker.com | sh",
      "sudo usermod -aG docker ubuntu",
      "sudo systemctl enable docker",
      "sudo systemctl start docker",
      "docker --version",
      "echo 'Docker installed successfully'"
    ]
  }

  # Create saga directory structure
  provisioner "remote-exec" {
    inline = [
      "mkdir -p ~/saga/data/postgres",
      "mkdir -p ~/saga/data/kafka",
      "mkdir -p ~/saga/data/zookeeper",
      "mkdir -p ~/saga/logs"
    ]
  }

  # Copy docker-compose files
  provisioner "file" {
    source      = "${path.module}/saga/"
    destination = "/home/ubuntu/saga"
  }

  # Login to GHCR (uses environment variable for token)
  provisioner "remote-exec" {
    inline = [
      "echo '${var.ghcr_token}' | sudo docker login ghcr.io -u ${var.ghcr_username} --password-stdin",
      "echo 'GHCR login successful'"
    ]
  }

  # Pull images ahead of time
  provisioner "remote-exec" {
    inline = [
      "sudo docker compose -f ~/saga/docker-compose.infra.yml pull",
      "sudo docker compose -f ~/saga/docker-compose.choreography.yml pull",
      "sudo docker compose -f ~/saga/docker-compose.orchestration.yml pull",
      "echo 'All images pulled successfully'"
    ]
  }

  # Start infrastructure services
  provisioner "remote-exec" {
    inline = [
      "cd ~/saga && sudo docker compose -f docker-compose.infra.yml up -d",
      "sleep 30",
      "sudo docker compose -f ~/saga/docker-compose.infra.yml ps",
      "echo 'Infrastructure services started'",
      "echo 'Kafka: ${var.nodes["saga-node"].ip_address}:9092'",
      "echo 'PostgreSQL: ${var.nodes["saga-node"].ip_address}:5432'"
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
# Observability Node Setup (Prometheus, Grafana, Zipkin)
#------------------------------------------------------------------------------

resource "null_resource" "observability_node" {
  depends_on = [lxd_instance.node]

  connection {
    type        = "ssh"
    user        = "ubuntu"
    host        = var.nodes["observability-node"].ip_address
    private_key = file(var.ssh_private_key_path)
    agent       = false
    timeout     = "10m"
  }

  # Wait for cloud-init to complete
  provisioner "remote-exec" {
    inline = [
      "cloud-init status --wait",
      "echo 'Cloud-init completed on observability-node'"
    ]
  }

  # Install Docker
  provisioner "remote-exec" {
    inline = [
      "curl -fsSL https://get.docker.com | sh",
      "sudo usermod -aG docker ubuntu",
      "sudo systemctl enable docker",
      "sudo systemctl start docker",
      "docker --version",
      "echo 'Docker installed successfully'"
    ]
  }

  # Create observability directory structure
  provisioner "remote-exec" {
    inline = [
      "mkdir -p ~/observability/prometheus",
      "mkdir -p ~/observability/grafana/provisioning/datasources",
      "mkdir -p ~/observability/grafana/provisioning/dashboards",
      "mkdir -p ~/observability/grafana/dashboards",
      "mkdir -p ~/observability/zipkin"
    ]
  }

  # Copy configuration files
  provisioner "file" {
    source      = "${path.module}/observability/"
    destination = "/home/ubuntu/observability"
  }

  # Start observability stack
  provisioner "remote-exec" {
    inline = [
      "cd ~/observability && sudo docker compose up -d",
      "sleep 15",
      "sudo docker compose -f ~/observability/docker-compose.yml ps",
      "echo 'Observability stack started successfully'",
      "echo 'Prometheus: http://${var.nodes["observability-node"].ip_address}:9090'",
      "echo 'Grafana: http://${var.nodes["observability-node"].ip_address}:3000 (admin/admin)'",
      "echo 'Zipkin: http://${var.nodes["observability-node"].ip_address}:9411'"
    ]
  }
}
