# Título do projeto

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen.svg)](https://github.com/taylorteixeira/projeto-ed-satc)  
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue)](https://jlsilva01.github.io/projeto-ed-satc/)

Repositorio para desenvolvimento do projeto final da disciplina de Engenharia de Dados do curso de Engenharia de Software da UNISATC.

## Desenho de Arquitetura

Coloque uma imagem do seu projeto, como no exemplo abaixo:

![image](https://github.com/jlsilva01/projeto-ed-satc/assets/484662/541de6ab-03fa-49b3-a29f-dec8857360c1)

## Pré-requisitos e ferramentas utilizadas

- **Linguagem:** Python 3.11+
- **Gerenciador de dependências:** Poetry
- **Banco de Dados:** MongoDB
- **Qualidade de código:** pre-commit (ruff, black, isort, flake8, mypy)
- **Infraestrutura e Orquestração:**
   - Azure Data Lake Storage (ADLS)
   - Azure Data Factory (ADF)
   - Databricks
- **Documentação:** MkDocs + mkdocstrings + mkdocs-material


## Instalação

### 1. Clonar o repositório

```bash
git clone https://github.com/taylorteixeira/projeto-ed-satc.git
cd projeto-ed-satc
```

### 2. Instalar dependências

```bash
poetry install
```

### 3. Executar localmente

#### Antes de começar, você precisará ter as seguintes ferramentas instaladas no seu computador:


- [Azure CLI](https://learn.microsoft.com/pt-br/cli/azure/)
- [Visual Studio Code](https://code.visualstudio.com/download)
- [Terraform](https://www.terraform.io/downloads)
- [Poetry](https://python-poetry.org/)
- Uma conta de e-mail Microsoft específica para esta atividade

Além disso, é necessário possuir o **[MS Learn Sandbox](https://learn.microsoft.com/pt-br/training/modules/build-serverless-api-with-functions-api-management/5-exercise-import-additional-functions-existing-api-gateway?ns-enrollment-type=learningpath&ns-enrollment-id=learn.create-serverless-applications)** para ativar uma assinatura de testes gratuita.

---

#### Passo 1: Inicializar o Azure Data Lake com Terraform

#### Para levantar o **Data Lake**, siga os comandos abaixo:

1. Navegue até a pasta `iac/adls`: 


```bash
cd iac/adls
```

2. Siga o roteiro do repositório **jlsilva01/adls-azure** para criar o Azure Data Lake Storage gratuitamente.

#### Aqui estão os comandos necessários:

#### 1. Efetue login no Azure: 


```bash
az login
```

#### 2. Utilize a assinatura gratuita:

```bash
az account set --subscription "Concierge Subscription"
```

#### 3. Ajuste a variável `resource_group_name` no arquivo `variables.tf` com o nome do **Resource Group** usado:

```terraform
variable "resource_group_name" {
    default = "learn-877e311a-66ab-401b-9372-06326c9bd083"
}
```

#### 4. Execute os comandos do Terraform na seguinte ordem:

- Inicializar o Terraform:

   ```bash
   terraform init
   ```

- Validar os arquivos do Terraform:

  ```bash
  terraform validate
  ```

- Ajustar o formato dos arquivos:

  ```bash
  terraform fmt
  ```

- Gerar um plano de implantação:

  ```bash
  terraform plan
  ```

- Implantar na cloud:
  ```bash
  terraform apply
  ```

#### 5. Confirme no portal do Azure:

| Acesse [portal.azure.com](https://portal.azure.com/) para validar a criação do **Azure Data Lake Storage Gen2**. |
| ---------------------------------------------------------------------------------------------------------------- |

#### 6. (Opcional) Para remover os recursos criados após os testes:

```bash
terraform destroy
```

---

#### Passo 2: Configurar o pipeline MongoDB

1. Retorne à raiz do projeto, se necessário, no terminal:

   ```bash
   cd ../../
   ```

2. Rode o pipeline de ETL que configura o banco de dados MongoDB:
   ```bash
   poetry run python iac/mongo/elt_mongodb_n_collections.py
   ```

---

### Passo 3: Executar os notebooks no Databricks

1. Suba os notebooks localizados na pasta:

   ```
   iac/databriks
   ```

2. Configure e execute os notebooks diretamente no **Databricks**, conectando ao pipeline e verificando os dados processados.

## Documentação (MkDocs)

Toda a documentação está em `docs/`:

```bash
uv run mkdocs build
uv run mkdocs serve
```

Acesse o site em `http://127.0.0.1:8000`.

Para publicar o site estático:

```bash
uv run mkdocs gh-deploy
```

## Colaboração

1. Abra uma **issue** para discutir sua feature ou bug.
2. Crie um **branch**:

   ```bash
   git checkout -b feature/nome-da-sua-feature
   ```

3. Faça suas alterações e **commit** seguindo o [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
4. Envie um **pull request** para `main`.
5. Aguarde revisão e merge.

## Versão

Fale sobre a versão e o controle de versões para o projeto.

## Autores

Mencione todos aqueles que ajudaram a levantar o projeto desde o seu início

- **Taylor Teixeira** - _Função_ - [https://github.com/taylorteixeira](https://github.com/taylorteixeira)
- **Eduardo Ribarski** - _Função_ - [https://github.com/ribarski](https://github.com/ribarski)
- **Eryc Jacinto** - _Função_ - [https://github.com/ErycMJ](https://github.com/ErycMJ)
- **Edrik Steiner** - _Função_ - [https://github.com/edrikfsteiner](https://github.com/edrikfsteiner)
- **Igor Steiner** - _Documentação_ - [https://github.com/IgorSteinerS](https://github.com/IgorSteinerS)

## Licença

Este projeto está sob a licença MIT - veja o arquivo [LICENSE](https://github.com/taylorteixeira/projeto-ed-satc/blob/main/LICENSE) para detalhes.  
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Referências

- [Template para o Projeto](https://github.com/jlsilva01/projeto-ed-satc) - jlsilva01
- [alds-azure](https://github.com/jlsilva01/adls-azure) - jlsilva01
- [engenharia-dados-azure-databricks](https://github.com/jlsilva01/engenharia-dados-azure-databricks) - jlsilva01