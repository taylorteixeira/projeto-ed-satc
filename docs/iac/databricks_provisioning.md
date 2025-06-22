


# Azure Databricks Workspace (Provisionamento com Terraform)

Esta seção documenta a Infraestrutura como Código (IaC) para o provisionamento de um **Azure Databricks Workspace** e seus recursos de rede associados (Resource Group, Virtual Network e Subnets) utilizando Terraform. O objetivo é automatizar e padronizar a criação do ambiente Databricks, garantindo que ele seja configurado com as melhores práticas de segurança e conectividade de rede, como a injeção em VNet.

---

## Estrutura do Módulo Terraform (Databricks)

O módulo Terraform para o Azure Databricks Workspace está localizado na pasta `iac/databricks/` e é composto pelos seguintes arquivos, cada um com uma responsabilidade específica:

*   `main.tf`: Define os recursos Azure que serão provisionados (Resource Group, Databricks Workspace, Virtual Network, Subnets).
*   `output.tf`: Exporta informações importantes do workspace Databricks criado, como o ID e a URL.
*   `provider.tf`: Configura os provedores Terraform necessários (`azurerm` e `databricks`) e suas autenticações.
*   `variables.tf`: Declara as variáveis de entrada que permitem a parametrização do deployment, como nomes e credenciais.

### `main.tf` - Definição dos Recursos do Databricks e Rede

Este arquivo central descreve a infraestrutura principal do Databricks. Ele cria um Resource Group dedicado para organizar os recursos, o próprio Databricks Workspace com a SKU `premium` (que habilita funcionalidades empresariais e de segurança avançadas, como controle de acesso baseado em IP e conectividade privada), e uma Virtual Network (VNet) com duas subnets específicas para a injeção do Databricks. A injeção em VNet é fundamental para garantir o isolamento de rede e a comunicação segura com outros recursos.

```terraform
resource "azurerm_resource_group" "iac-rg" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_virtual_network" "iac-vnet" {
  name                = "iac-vnet"
  address_space       = ["10.0.0.0/16"] # Define o espaço de endereçamento da VNet
  location            = azurerm_resource_group.iac-rg.location
  resource_group_name = azurerm_resource_group.iac-rg.name
}

# Subnet pública para o Databricks (control plane)
resource "azurerm_subnet" "iac-public-subnet" {
  name                 = "iac-public-subnet"
  resource_group_name  = azurerm_resource_group.iac-rg.name
  virtual_network_name = azurerm_virtual_network.iac-vnet.name
  address_prefixes     = ["10.0.1.0/24"] # Espaço de endereçamento para a subnet pública
}

# Subnet privada para o Databricks (data plane)
resource "azurerm_subnet" "iac-private-subnet" {
  name                 = "iac-private-subnet"
  resource_group_name  = azurerm_resource_group.iac-rg.name
  virtual_network_name = azurerm_virtual_network.iac-vnet.name
  address_prefixes     = ["10.0.2.0/24"] # Espaço de endereçamento para a subnet privada
}

resource "azurerm_databricks_workspace" "iac-databricks" {
  name                       = var.workspace_name
  location                   = azurerm_resource_group.iac-rg.location
  resource_group_name        = azurerm_resource_group.iac-rg.name
  sku                        = "premium" # Premium SKU oferece recursos como conectividade privada (VNet Injection)

  # Configuração para VNet Injection
  custom_public_subnet_name  = azurerm_subnet.iac-public-subnet.name
  custom_private_subnet_name = azurerm_subnet.iac-private-subnet.name
  virtual_network_id         = azurerm_virtual_network.iac-vnet.id
}
```
\
`src/iac/databricks/main.tf`


**Observações sobre a Rede (VNet Injection):**
A injeção do Databricks em uma rede virtual (VNet Injection) é uma funcionalidade avançada que permite implantar o Azure Databricks em uma VNet existente que você gerencia. Isso oferece os seguintes benefícios:
*   **Conectividade Privada:** Permite que o Databricks se conecte de forma privada e segura a outros serviços Azure (ex: ADLS, SQL Database, Key Vault) e a recursos on-premises via VPN ou ExpressRoute, sem tráfego passando pela internet pública.
*   **Segurança Reforçada:** Permite o controle de acesso de rede usando NSGs (Network Security Groups) e rotas definidas pelo usuário (UDRs) para filtrar o tráfego de entrada e saída, isolando o Databricks dentro da sua rede corporativa.
*   **Extensão da Rede Corporativa:** O Databricks opera como uma extensão da sua rede virtual corporativa, permitindo que você aplique suas próprias políticas de segurança, monitoramento e auditoria.
*   **SKU Premium:** A VNet Injection é uma funcionalidade exclusiva da SKU `premium` do Azure Databricks. Outras SKUs (Standard, Trial) não suportam essa capacidade de rede avançada.
*   **Subnets Dedicadas:** O Databricks requer duas subnets para a injeção em VNet: uma subnet pública (para o plano de controle) e uma subnet privada (para o plano de dados). Ambas devem ter um prefixo de endereço CIDR mínimo de `/26` e não devem ser usadas por outros recursos. O Terraform, neste exemplo, cria essas duas subnets e as associa ao workspace.

### `output.tf` - Saídas do Módulo

O arquivo `output.tf` define os valores que serão expostos pelo módulo Terraform após a conclusão do provisionamento. Essas saídas são úteis para referenciar o Databricks Workspace em outros módulos Terraform (por exemplo, para configurar o Databricks Provider em um módulo diferente), scripts de automação ou para simplesmente obter informações essenciais após o deploy para acesso manual ou integração com sistemas de CI/CD.

```terraform
output "databricks_workspace_id" {
  description = "The ID of the Azure Databricks Workspace"
  value       = azurerm_databricks_workspace.iac-databricks.id
}

output "databricks_workspace_url" {
  description = "The URL of the Azure Databricks Workspace"
  value       = azurerm_databricks_workspace.iac-databricks.workspace_url
}
```
\
`src/iac/databricks/output.tf`


### `provider.tf` - Configuração dos Provedores Terraform

Este arquivo configura os provedores `azurerm` e `databricks`, que são essenciais para interagir com os serviços Azure e com o próprio Databricks, respectivamente. A especificação da `source` e `version` para cada provedor garante que o Terraform utilize a versão correta e estável, evitando problemas de compatibilidade. A autenticação com o Azure é feita através do `subscription_id`, enquanto a autenticação com o Databricks é realizada usando credenciais de uma Service Principal, o que é uma prática recomendada para automação e segurança.

```terraform
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.9.0" # Versão do provedor Azure - sempre fixe a versão para reprodutibilidade
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0.0" # Versão do provedor Databricks - sempre fixe a versão para reprodutibilidade
    }
  }
}

provider "azurerm" {
  features {} # Habilita funcionalidades opcionais do provedor, como o gerenciamento de recursos em escopo de "feature flags".
  subscription_id = var.subscription_id # ID da assinatura Azure
}

provider "databricks" {
  azure_workspace_resource_id = azurerm_databricks_workspace.iac-databricks.id # ID do workspace Databricks para autenticação
  azure_client_id             = var.azure_client_id     # Client ID da Service Principal
  azure_client_secret         = var.azure_client_secret # Client Secret da Service Principal
  azure_tenant_id             = var.azure_tenant_id     # Tenant ID da organização
}
```
\
`src/iac/databricks/provider.tf`


**Autenticação do Provedor Databricks:**
A autenticação do provedor Databricks é configurada para usar uma Service Principal do Azure AD. Isso é uma prática de segurança robusta para automação, evitando a necessidade de credenciais de usuário. As variáveis `azure_client_id`, `azure_client_secret` e `azure_tenant_id` devem ser fornecidas, e o `azure_workspace_resource_id` garante que o provedor interaja com o workspace Databricks provisionado.

### `variables.tf` - Variáveis de Entrada

Este arquivo declara todas as variáveis de entrada necessárias para o módulo Terraform, permitindo que o deployment seja parametrizado e reutilizável. Inclui variáveis para nomes de recursos, localização e, crucialmente, as credenciais da Service Principal necessárias para a autenticação.

```terraform
variable "resource_group_name" {
  description = "Nome do Resource Group do Databricks"
  type        = string
}

variable "location" {
  description = "Localização (região Azure) onde o Databricks será provisionado."
  type        = string
  default     = "East US" # Valor padrão pode ser usado se não for explicitamente fornecido
}

variable "workspace_name" {
  description = "Nome único para o Workspace do Azure Databricks."
  type        = string
}

variable "azure_client_id" {
  description = "Client ID (App ID) da Service Principal Azure para autenticação."
  type        = string
}

variable "azure_client_secret" {
  description = "Client Secret (senha) da Service Principal Azure. Marque como sensível para segurança."
  type        = string
  sensitive   = true # Marca a variável como sensível para evitar que seu valor seja exibido em logs/outputs
}

variable "azure_tenant_id" {
  description = "Tenant ID (Directory ID) da sua organização Azure AD."
  type        = string
}

variable "subscription_id" {
  description = "ID da Subscription Azure onde os recursos serão provisionados."
  type        = string
}
```
\
`src/iac/databricks/variables.tf`



