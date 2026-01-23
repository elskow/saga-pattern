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
#   - k6-runner: Load testing node (k6)
#   - observability-node: Monitoring (Prometheus, Grafana, Zipkin)
#------------------------------------------------------------------------------

nodes = {
  "saga-node" = {
    vcpu       = 22
    memory     = "40GB"
    storage    = "260GB"
    ip_address = "192.168.11.152"
  }
  "k6-runner" = {
    vcpu       = 4
    memory     = "4GB"
    storage    = "20GB"
    ip_address = "192.168.11.158"
  }
  "observability-node" = {
    vcpu       = 4
    memory     = "6GB"
    storage    = "120GB"
    ip_address = "192.168.11.160"
  }
}
