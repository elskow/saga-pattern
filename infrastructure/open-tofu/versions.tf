terraform {
  required_version = ">= 1.6.0"

  required_providers {
    lxd = {
      source  = "terraform-lxd/lxd"
      version = "~> 2.6.1"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    external = {
      source  = "hashicorp/external"
      version = "~> 2.3"
    }
  }
}

provider "lxd" {
  # Generate client certificates automatically if they don't exist
  generate_client_certificates = true

  # Accept remote certificate on first connection
  # Set to false if you want to verify the certificate manually first
  accept_remote_certificate = true

  remote {
    name    = var.lxd_remote_name
    address = var.lxd_remote_address
    default = true
  }
}
