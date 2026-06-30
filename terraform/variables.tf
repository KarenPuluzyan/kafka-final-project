variable "yc_token" {
  description = "IAM-токен Yandex Cloud (yc iam create-token)"
  type        = string
  sensitive   = true
}

variable "yc_cloud_id" {
  description = "ID облака (yc config get cloud-id)"
  type        = string
}

variable "yc_folder_id" {
  description = "ID каталога (yc config get folder-id)"
  type        = string
}

variable "yc_zone" {
  description = "Зона доступности"
  type        = string
  default     = "ru-central1-a"
}

variable "ssh_public_key_path" {
  description = "Путь к публичному SSH-ключу на локальной машине"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}

variable "repo_url" {
  description = "URL репозитория GitHub с проектом"
  type        = string
  default     = "https://github.com/KarenPuluzyan/kafka-final-project.git"
}
