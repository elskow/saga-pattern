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
#------------------------------------------------------------------------------

nodes = {
  "k3s-server" = {
    vcpu       = 6
    memory     = "8GB"
    storage    = "60GB"
    ip_address = "192.168.11.152"
  }
  "k3s-agent-1" = {
    vcpu       = 8
    memory     = "16GB"
    storage    = "100GB"
    ip_address = "192.168.11.154"
  }
  "k3s-agent-2" = {
    vcpu       = 8
    memory     = "16GB"
    storage    = "100GB"
    ip_address = "192.168.11.156"
  }
  "k6-runner" = {
    vcpu       = 4
    memory     = "4GB"
    storage    = "20GB"
    ip_address = "192.168.11.158"
  }
}
