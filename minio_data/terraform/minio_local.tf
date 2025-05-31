terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.1" # Verifique a versão mais recente
    }
  }
}

provider "docker" {}

resource "docker_image" "minio_image" {
  name         = "quay.io/minio/minio:latest"
  keep_locally = true # Mantém a imagem baixada localmente
}

resource "docker_container" "minio_server_tf" {
  image = docker_image.minio_image.image_id
  name  = "minio-tf-local" # Nome do contêiner
  ports {
    internal = 9000
    external = 9000
  }
  ports {
    internal = 9001
    external = 9001
  }
  env = [
    "MINIO_ROOT_USER=minioadmin",
    "MINIO_ROOT_PASSWORD=minioadmin"
  ]
  command = ["server", "/data", "--console-address", ":9001"]

  # Opcional: Montar um volume local para persistência de dados do MinIO
  # Certifique-se que o diretório local exista (ex: ./minio_data_tf)
  # volumes {
  #   host_path      = abspath("./minio_data_tf")
  #   container_path = "/data"
  # }
}

output "minio_endpoint" {
  value = "http://localhost:9000"
}

output "minio_console" {
  value = "http://localhost:9001"
}

output "minio_access_key" {
  value = "minioadmin"
  sensitive = true
}

output "minio_secret_key" {
  value = "minioadmin"
  sensitive = true
}