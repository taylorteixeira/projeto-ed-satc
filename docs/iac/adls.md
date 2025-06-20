# Azure Data Lake Storage (Terraform)

Esta seção detalha a configuração da infraestrutura do Azure Data Lake Storage Gen2 (ADLS Gen2) utilizando Terraform. O objetivo é provisionar os containers necessários para as diferentes camadas do pipeline de dados (Landing Zone, Bronze, Silver e Gold), garantindo uma estrutura organizada, padronizada e otimizada para o processamento de dados em larga escala.

A documentação a seguir cobre os principais arquivos de configuração do Terraform, explicando a função de cada um e um guia passo a passo para a implantação e gerenciamento desses recursos no Azure, com foco em boas práticas e considerações de ambiente.

---

## Estrutura do Módulo Terraform (ADLS)

O módulo Terraform para o ADLS está localizado em `iac/adls/` e é composto pelos seguintes arquivos, cada um com uma responsabilidade específica na definição da infraestrutura:

*   `main.tf`: Contém a definição dos recursos Azure a serem criados, neste caso, os containers do ADLS Gen2.
*   `output.tf`: Define os valores que serão expostos após a aplicação do Terraform, facilitando a integração com outros módulos ou a consulta de informações.
*   `provider.tf`: Configura o provedor Azure (`azurerm`), especificando a versão e as funcionalidades necessárias para interagir com os serviços do Azure.
*   `variables.tf`: Declara as variáveis de entrada utilizadas pelo módulo, permitindo a parametrização e reutilização do código Terraform em diferentes ambientes ou contextos.

### `main.tf` - Definição dos Containers

Este arquivo é o coração do módulo, responsável por instanciar os recursos do Azure. Aqui, são criados os quatro containers (Landing Zone, Bronze, Silver e Gold) dentro de uma Storage Account existente. A utilização de containers distintos para cada camada do pipeline de dados (Landing Zone, Bronze, Silver, Gold) é uma prática recomendada para organizar os dados com base em seu estágio de processamento e qualidade, facilitando a governança e o acesso.

```markdown
# Define uma variável local para o nome da Storage Account existente.
# Em um módulo Terraform mais complexo, 'storage_account_name' seria tipicamente
# uma variável de entrada definida em 'variables.tf' e passada para este módulo.
variable "storage_account_name" {
  type        = string
  description = "The name of the existing Azure Storage Account where containers will be created."
  default     = "armazensatc" # Valor padrão para fins de demonstração
}

# Cria o container 'landing-zone'
# Destinado a dados brutos, sem transformações, diretamente da fonte.
resource "azurerm_storage_container" "landing-zone" {
  name                  = "landing-zone"
  storage_account_name  = var.storage_account_name
  container_access_type = "private" # Garante que o acesso é restrito e requer autenticação.
}

# Cria o container 'bronze'
# Armazena dados brutos, mas já com alguma estrutura ou formato padronizado (ex: Parquet).
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = var.storage_account_name
  container_access_type = "private"
}

# Cria o container 'silver'
# Contém dados limpos, transformados e enriquecidos, prontos para análise.
resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = var.storage_account_name
  container_access_type = "private"
}

# Cria o container 'gold'
# Armazena dados agregados e prontos para consumo por aplicações de BI ou Machine Learning.
resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = var.storage_account_name
  container_access_type = "private"
}
```

<br>
```src/iac/adls/main.tf```
<br>


### `output.tf` - Saídas do Módulo
O arquivo output.tf define os valores que o módulo Terraform exportará após a conclusão da aplicação. Isso é extremamente útil para:

1. Integração: Permitir que outros módulos Terraform ou scripts acessem informações sobre os recursos criados.
2. Validação: Facilitar a verificação programática dos nomes dos recursos provisionados.
3. Visibilidade: Fornecer um resumo claro dos nomes dos containers criados diretamente na saída do comando terraform apply.

```markdown
output "adls_container_landing_zone" {
  description = "Name of the created storage container landing-zone"
  value       = azurerm_storage_container.landing-zone.name
}

output "adls_container_bronze" {
  description = "Name of the created storage container bronze"
  value       = azurerm_storage_container.bronze.name
}

output "adls_container_silver" {
  description = "Name of the created storage container silver"
  value       = azurerm_storage_container.silver.name
}

output "adls_container_gold" {
  description = "Name of the created storage container gold"
  value       = azurerm_storage_container.gold.name
}
```
<br>
```src/iac/adls/output.tf```
<br>

### `provider.tf` - Configuração do Provedor Azure
Este arquivo é responsável por configurar o provedor que o Terraform usará para interagir com a infraestrutura cloud. No caso, o provedor `azurerm` é especificado, juntamente com sua versão mínima exigida. A fixação da versão (`version = "3.3.0"`) é uma boa prática para garantir a reprodutibilidade do deploy e evitar que atualizações de provedor com mudanças incompatíveis (breaking changes) afetem inesperadamente sua infraestrutura.

```terraform
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "3.3.0" # Fixa a versão para garantir consistência e evitar breaking changes.
    }
  }
}

# Configura o provedor Microsoft Azure.
# O bloco 'features {}' é usado para habilitar ou desabilitar funcionalidades específicas do provedor,
# embora muitas vezes seja deixado vazio para usar as configurações padrão.
provider "azurerm" {
  features {}
}
```
<br>
```src/iac/adls/provider.tf```
<br>

### `variables.tf` - Variáveis de Entrada
Este arquivo declara as variáveis de entrada que permitem a parametrização do módulo Terraform. A utilização de variáveis é fundamental para tornar o código reutilizável e adaptável a diferentes ambientes ou configurações sem a necessidade de modificar o código-fonte diretamente. Neste caso, são definidas variáveis para o Resource Group e a Localização (região Azure).

```markdown
# Lista de variaveis utilizadas nos arquivos de terraform
variable "resource_group_name" {
  type        = string
  description = "The name of the Azure Resource Group where resources will be deployed."
  default     = "databricks-rg-eng-dados-i45pzpkirdaeo" # Valor padrão para fins de demonstração
}

variable "location" {
  type        = string
  description = "The Azure region where resources will be deployed (e.g., 'eastus', 'brazilsouth')."
  default     = "eastus" # Valor padrão para fins de demonstração
}
```
<br>
```src/iac/adls/variables.tf```
<br>