resource "azurerm_resource_group" "iac-rg" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_databricks_workspace" "iac-databricks" {
  name                = var.workspace_name
  location            = azurerm_resource_group.iac-rg.location
  resource_group_name = azurerm_resource_group.iac-rg.name
  sku                 = "premium"
}

resource "azurerm_virtual_network" "iac-vnet" {
  name                = "iac-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.iac-rg.location
  resource_group_name = azurerm_resource_group.iac-rg.name
}

resource "azurerm_subnet" "iac-subnet" {
  name                 = "iac-subnet"
  resource_group_name  = azurerm_resource_group.iac-rg.name
  virtual_network_name = azurerm_virtual_network.iac-vnet.name
  address_prefixes     = ["10.0.1.0/24"]
}