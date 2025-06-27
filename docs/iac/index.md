# Infraestrutura como Código (IaC)

Bem-vindo à seção de Infraestrutura como Código (IaC) do nosso projeto. Aqui você encontrará a documentação detalhada sobre como os recursos de infraestrutura são provisionados, configurados e gerenciados de forma automatizada e versionada.

Nesta seção, abordamos a configuração da nossa infraestrutura de dados no ambiente Azure, com foco nos principais componentes que sustentam nosso pipeline de dados Lakehouse:

---

## Azure Data Lake Storage (ADLS) com Terraform

Esta subseção descreve a definição e implantação do nosso **Azure Data Lake Storage Gen2 (ADLS Gen2)** utilizando o Terraform. O ADLS Gen2 é a base do nosso Data Lake, fornecendo escalabilidade massiva para petabytes de dados, baixo custo de armazenamento e um namespace hierárquico otimizado para cargas de trabalho analíticas e Big Data, como Spark e Hadoop. Detalhamos a estrutura dos nossos containers de dados (`Landing Zone`, `Bronze`, `Silver`, `Gold`).

A documentação inclui:
*   **Estrutura de diretórios:** Como os dados são organizados logicamente dentro do Data Lake para facilitar o acesso, o processamento e a aplicação de políticas de retenção e segurança.
*   **Arquivos de configuração do Terraform:** Detalhes de `main.tf` (recursos principais), `output.tf` (valores de saída), `provider.tf` (configuração do provedor Azure) e `variables.tf` (parâmetros de entrada), e como eles definem a infraestrutura de armazenamento, incluindo configurações de segurança, acesso (RBAC) e redundância.


É o alicerce do nosso Data Lake, onde todos os dados, desde os brutos até os prontos para consumo, são armazenados de forma segura e acessível, servindo como a fonte única da verdade para nossas cargas de trabalho analíticas.

[Acesse a documentação detalhada do ADLS aqui.](adls.md)

---

## 🚀 Azure Databricks Workspace (Provisionamento com Terraform)

Esta subseção documenta a Infraestrutura como Código (IaC) para o provisionamento do **Azure Databricks Workspace** e seus recursos de rede associados (Resource Group, Virtual Network e Subnets) utilizando Terraform. A automação do setup do Databricks via Terraform é crucial para garantir ambientes consistentes, seguros e replicáveis, especialmente com a configuração de **VNet Injection**. A VNet Injection permite que o Databricks Workspace seja implantado dentro da sua própria Rede Virtual (VNet), proporcionando isolamento de rede, conectividade privada com outros serviços Azure (como ADLS, Azure SQL Database) via Private Link e a aplicação de regras de segurança de rede (NSGs) para controle de tráfego.

A documentação aborda:
*   **Definição de Recursos:** Explica como o `main.tf` cria o Resource Group, a VNet com subnets dedicadas (para `public` e `private` Databricks subnets) e o Databricks Workspace na **SKU Premium**, que oferece recursos avançados como controle de acesso baseado em função (RBAC), auditoria e VNet Injection.
*   **Configuração de Provedores:** Detalha como os provedores `azurerm` (para recursos Azure) e `databricks` (para recursos específicos do Databricks, como clusters e notebooks) são configurados para interagir com o Azure e o próprio serviço Databricks, respectivamente.
*   **Variáveis de Entrada:** Descreve as variáveis necessárias para parametrizar o deploy, incluindo `location`, `resource_group_name`, `vnet_address_prefix`, `databricks_workspace_name` e credenciais de **Service Principal** para automação segura e autenticação não interativa.

Este componente é a nossa plataforma de análise e engenharia de dados unificada, onde executamos pipelines ETL/ELT complexos, cargas de trabalho de Machine Learning e colaboramos em projetos de ciência de dados.

[Acesse a documentação detalhada do provisionamento do Databricks aqui.](databricks_provisioning.md)

