#------------------------------------------------------------------------------
# LXD Connection Variables
#------------------------------------------------------------------------------

variable "lxd_remote_address" {
  description = "LXD remote server address"
  type        = string
  default     = "https://192.168.10.21:8443"
}

variable "lxd_remote_name" {
  description = "Name for the LXD remote"
  type        = string
  default     = "helmy-labs"
}

variable "lxd_project" {
  description = "LXD project name"
  type        = string
  default     = "helmy-labs"
}

#------------------------------------------------------------------------------
# Node Specifications
#------------------------------------------------------------------------------

variable "nodes" {
  description = "Map of nodes to provision with their specifications"
  type = map(object({
    vcpu       = number
    memory     = string
    storage    = string
    ip_address = string
  }))
  default = {
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
}

#------------------------------------------------------------------------------
# GHCR (GitHub Container Registry) Credentials
#------------------------------------------------------------------------------

variable "ghcr_username" {
  description = "GitHub username for GHCR authentication"
  type        = string
  default     = "elskow"
}

variable "ghcr_token" {
  description = "GitHub PAT token for GHCR authentication"
  type        = string
  sensitive   = true
  default     = "" # Set via environment variable TF_VAR_ghcr_token or -var flag
}

#------------------------------------------------------------------------------
# Instance Configuration
#------------------------------------------------------------------------------

variable "instance_type" {
  description = "Instance type: 'container' or 'virtual-machine'"
  type        = string
  default     = "virtual-machine"
}

variable "instance_image" {
  description = "Base image for instances"
  type        = string
  default     = "ubuntu:24.04"
}

variable "network_bridge" {
  description = "Network bridge for eth0"
  type        = string
  default     = "br-vlan20"
}

variable "storage_pool" {
  description = "LXD storage pool name"
  type        = string
  default     = "ssd03"
}

variable "default_profile" {
  description = "Default LXD profile to use"
  type        = string
  default     = "default"
}

#------------------------------------------------------------------------------
# SSH Configuration
#------------------------------------------------------------------------------

variable "ssh_public_key" {
  description = "SSH public key for passwordless access"
  type        = string
  default     = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKa3TCYd92DAxazVO+zT+SszoFagOhJNtocJ6KAhxlAG helmyl.work@gmail.com"
}

variable "ssh_private_key_path" {
  description = "Path to SSH private key for provisioning"
  type        = string
  default     = "~/.ssh/id_ed25519"
}

#------------------------------------------------------------------------------
# Git Repository Configuration (for k6 thesis harness)
#------------------------------------------------------------------------------

variable "saga_repo_url" {
  description = "Git repository URL for saga-pattern project"
  type        = string
  default     = "https://github.com/Elskow/saga-pattern.git"
}

variable "saga_repo_branch" {
  description = "Git branch to clone"
  type        = string
  default     = "main"
}
