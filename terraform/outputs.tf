output "vm_public_ip" {
  description = "Публичный IP виртуальной машины"
  value       = yandex_vpc_address.public_ip.external_ipv4_address[0].address
}

output "ssh_command" {
  description = "Команда для подключения по SSH"
  value       = "ssh ubuntu@${yandex_vpc_address.public_ip.external_ipv4_address[0].address}"
}

output "services_urls" {
  description = "URL всех сервисов после запуска проекта"
  value = {
    kafka_ui_dc1 = "http://${yandex_vpc_address.public_ip.external_ipv4_address[0].address}:8080"
    kafka_ui_dc2 = "http://${yandex_vpc_address.public_ip.external_ipv4_address[0].address}:8081"
    grafana      = "http://${yandex_vpc_address.public_ip.external_ipv4_address[0].address}:3000"
    prometheus   = "http://${yandex_vpc_address.public_ip.external_ipv4_address[0].address}:9090"
    alertmanager = "http://${yandex_vpc_address.public_ip.external_ipv4_address[0].address}:9093"
    faust_api    = "http://${yandex_vpc_address.public_ip.external_ipv4_address[0].address}:6066/banned/list"
    connect_api  = "http://${yandex_vpc_address.public_ip.external_ipv4_address[0].address}:8083"
  }
}

output "cost_estimate" {
  description = "Ориентировочная стоимость (прерываемая VM)"
  value = {
    per_hour      = "~13 ₽/ч (без НДС)"
    for_24_hours  = "~310 ₽ за день (без НДС) / ~372 ₽ с НДС 20%"
    vm_type       = "Прерываемая (preemptible) — работает до 24 ч, затем YC останавливает"
    configuration = "16 vCPU / 64 ГБ RAM / 100 ГБ SSD / Intel Ice Lake"
  }
}

output "important_note" {
  description = "Важные напоминания"
  value = <<-NOTE
    ⚠️  ПРЕРЫВАЕМАЯ VM — важно знать:
    1. YC может остановить VM через 24 ч (или раньше при нехватке ресурсов в зоне)
    2. Данные на диске СОХРАНЯЮТСЯ после остановки (если не запускать terraform destroy)
    3. Для сбора результатов используйте: bash ~/kafka-project/scripts/collect-report.sh
    4. Сохраните результаты ДО истечения суток: scp ubuntu@<IP>:~/kafka-project/report/ ./
    5. После завершения работы запустите: terraform destroy (удалит VM и остановит списание)
  NOTE
}
