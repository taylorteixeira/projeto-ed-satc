# Define o nome do grupo de recursos como uma variável
variable "resource_group_name" {
  description = "rg-pether-park"
  type        = string
  default     = "rg-adls-terraform-demo"
}

# Define a localização como uma variável
variable "location" {
  description = "A localização onde os recursos serão criados."
  type        = string
  default     = "brazilsouth"
}