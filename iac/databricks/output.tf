output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.iac-databricks.id
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.iac-databricks.workspace_url
}
