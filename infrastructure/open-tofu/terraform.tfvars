#------------------------------------------------------------------------------
# LXD Connection Settings
#------------------------------------------------------------------------------

lxd_remote_address = "https://192.168.10.21:8443"
lxd_remote_name    = "helmy-labs"
lxd_project        = "helmy-labs"

#------------------------------------------------------------------------------
# Instance Configuration
#------------------------------------------------------------------------------

instance_type  = "virtual-machine"
instance_image = "ubuntu:24.04"
network_bridge = "br-vlan20"
storage_pool   = "ssd03"

#------------------------------------------------------------------------------
# Node Specifications
# 
# Architecture:
#   - saga-node: Main application node (Docker Compose for Saga services)
#   - saga-node: k6 load generation plus Docker Compose for Saga services
#   - observability-node: SigNoz observability stack
#------------------------------------------------------------------------------

nodes = {
  "saga-node" = {
    vcpu       = 22
    memory     = "40GB"
    storage    = "260GB"
    ip_address = "192.168.11.152"
  }
  "observability-node" = {
    vcpu       = 4
    memory     = "6GB"
    storage    = "120GB"
    ip_address = "192.168.11.160"
  }
}

#------------------------------------------------------------------------------
# Git Repository for k6 Thesis Harness
#------------------------------------------------------------------------------

saga_repo_url    = "https://github.com/Elskow/saga-pattern.git"
saga_repo_branch = "main"
