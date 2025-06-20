# Nome da Storage Account já existente
variable "storage_account_name" {
  type    = string
  default = "armazensatc"
}

# Criar quatro containers dentro do Azure Data Lake Storage Gen2: Landing-zone, Bronze, Silver e Gold
resource "azurerm_storage_container" "landing-zone" {
  name                  = "landing-zone"
  storage_account_name  = var.storage_account_name
  container_access_type = "private"
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = var.storage_account_name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = var.storage_account_name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = var.storage_account_name
  container_access_type = "private"
}

