# ============================================================
# Terraform — VM в Yandex Cloud для финального проекта
#
# Режим: ПРЕРЫВАЕМАЯ VM на 1 день для получения результатов
# Конфигурация: 16 vCPU / 64 ГБ RAM / 100 ГБ SSD
# Стоимость: ~310 ₽ за 24 часа (прерываемая)
# Платформа: Intel Ice Lake (standard-v3)
# ============================================================

terraform {
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = ">= 0.100"
    }
  }
  required_version = ">= 1.3"
}

provider "yandex" {
  token     = var.yc_token
  cloud_id  = var.yc_cloud_id
  folder_id = var.yc_folder_id
  zone      = var.yc_zone
}

# ── Сеть ─────────────────────────────────────────────────────
resource "yandex_vpc_network" "main" {
  name = "kafka-project-net"
}

resource "yandex_vpc_subnet" "main" {
  name           = "kafka-project-subnet"
  zone           = var.yc_zone
  network_id     = yandex_vpc_network.main.id
  v4_cidr_blocks = ["10.0.1.0/24"]
}

# ── Статический публичный IP ──────────────────────────────────
resource "yandex_vpc_address" "public_ip" {
  name = "kafka-project-ip"
  external_ipv4_address {
    zone_id = var.yc_zone
  }
}

# ── Security Group ────────────────────────────────────────────
resource "yandex_vpc_security_group" "kafka_sg" {
  name       = "kafka-project-sg"
  network_id = yandex_vpc_network.main.id

  dynamic "ingress" {
    for_each = [
      { port = 22,   desc = "SSH" },
      { port = 8080, desc = "Kafka UI DC1" },
      { port = 8081, desc = "Kafka UI DC2" },
      { port = 3000, desc = "Grafana" },
      { port = 9090, desc = "Prometheus" },
      { port = 9093, desc = "Alertmanager" },
      { port = 6066, desc = "Faust Filter API" },
      { port = 8083, desc = "Kafka Connect" },
    ]
    content {
      protocol       = "TCP"
      port           = ingress.value.port
      v4_cidr_blocks = ["0.0.0.0/0"]
      description    = ingress.value.desc
    }
  }

  # Kafka внешние порты ЦОД-1
  ingress {
    protocol       = "TCP"
    from_port      = 19092
    to_port        = 19097
    v4_cidr_blocks = ["0.0.0.0/0"]
    description    = "Kafka DC1 external"
  }
  # Kafka внешние порты ЦОД-2
  ingress {
    protocol       = "TCP"
    from_port      = 29092
    to_port        = 29096
    v4_cidr_blocks = ["0.0.0.0/0"]
    description    = "Kafka DC2 external"
  }

  egress {
    protocol       = "ANY"
    v4_cidr_blocks = ["0.0.0.0/0"]
    description    = "All outbound"
  }
}

# ── VM ────────────────────────────────────────────────────────
resource "yandex_compute_instance" "kafka_vm" {
  name        = "kafka-project-vm"
  platform_id = "standard-v3"   # Intel Ice Lake
  zone        = var.yc_zone

  resources {
    cores         = 16
    memory        = 64      # ГБ
    core_fraction = 100     # 100% гарантированная производительность ядра
  }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = 100        # ГБ SSD
      type     = "network-ssd"
    }
  }

  network_interface {
    subnet_id          = yandex_vpc_subnet.main.id
    security_group_ids = [yandex_vpc_security_group.kafka_sg.id]
    nat                = true
    nat_ip_address     = yandex_vpc_address.public_ip.external_ipv4_address[0].address
  }

  scheduling_policy {
    # ПРЕРЫВАЕМАЯ VM:
    # - в ~3 раза дешевле обычной
    # - гарантированно работает минимум 1 час
    # - YC может остановить её через 24 часа (или раньше при нехватке ресурсов)
    # - для однодневного сбора результатов — идеальный выбор
    preemptible = true
  }

  metadata = {
    user-data = templatefile("${path.module}/cloud-init.yaml", {
      ssh_public_key = file(var.ssh_public_key_path)
      repo_url       = var.repo_url
    })
    ssh-keys = "ubuntu:${file(var.ssh_public_key_path)}"
  }

  labels = {
    project  = "kafka-final"
    env      = "study"
    duration = "1-day"
  }
}

data "yandex_compute_image" "ubuntu" {
  family = "ubuntu-2204-lts"
}
