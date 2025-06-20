# Infraestrutura como Código (IaC)

Bem-vindo à seção de Infraestrutura como Código (IaC) do nosso projeto. Aqui você encontrará a documentação detalhada sobre como os recursos de infraestrutura são provisionados, configurados e gerenciados.

Nesta seção, abordamos a configuração da nossa infraestrutura de dados no ambiente Azure, com foco nos principais componentes que sustentam nosso pipeline de dados Lakehouse:

---

## Azure Data Lake Storage (ADLS) com Terraform

Esta subseção descreve a definição e implantação do nosso **Azure Data Lake Storage Gen2 (ADLS Gen2)** utilizando o Terraform. O ADLS Gen2 é a base do nosso Data Lake, fornecendo escalabilidade massiva, baixo custo e um namespace hierárquico otimizado para cargas de trabalho analíticas e Big Data. Detalhamos a estrutura dos nossos containers de dados (`Landing Zone`, `Bronze`, `Silver`, `Gold`), que seguem as melhores práticas para organização de dados em um Data Lake, facilitando a governança e o acesso.

A documentação inclui:
*   **Estrutura de diretórios:** Como os dados são organizados logicamente dentro do Data Lake para facilitar o acesso, o processamento e a aplicação de políticas de retenção.
*   **Arquivos de configuração do Terraform:** Detalhes de `main.tf`, `output.tf`, `provider.tf`, `variables.tf` e como eles definem a infraestrutura de armazenamento, incluindo configurações de segurança e acesso.


É o alicerce do nosso Data Lake, onde todos os dados, desde os brutos até os prontos para consumo, são armazenados de forma segura e acessível.

[Acesse a documentação detalhada do ADLS aqui.](adls.md)

---

## Notebooks Databricks: Pipeline de Dados Lakehouse

Esta seção abrange a documentação dos notebooks PySpark executados no ambiente Databricks, que implementam as camadas do nosso pipeline de dados Lakehouse. O Databricks, com o Delta Lake como formato de armazenamento padrão, permite a construção de uma arquitetura Lakehouse que combina a flexibilidade e escalabilidade de um Data Lake com a confiabilidade, performance e recursos de transação (ACID) de um Data Warehouse tradicional.

Explicamos o papel de cada notebook (`Bronze`, `Silver`, `Gold`) na transformação e curadoria dos dados:
*   **Bronze Layer:** Responsável pela ingestão de dados brutos (`raw data`) de diversas fontes, mantendo o histórico e a fidelidade da fonte original. Os dados são carregados "as-is" para garantir a rastreabilidade.
*   **Silver Layer:** Foca no refinamento e limpeza dos dados. Aqui são aplicadas transformações básicas, validações, deduplicações e padronizações, resultando em dados mais limpos e consistentes.
*   **Gold Layer:** Camada de curadoria e agregação, onde os dados são transformados em modelos de dados otimizados para consumo analítico. Tabelas agregadas e dimensões/fatos são criadas para atender a necessidades de Business Intelligence (BI), relatórios e Machine Learning.

É aqui que o processamento de dados acontece, transformando dados brutos em insights valiosos e estruturados, prontos para serem consumidos por aplicações e usuários de negócios.

[Acesse a documentação detalhada dos Notebooks Databricks aqui.](databricks_notebooks.md)

---

## Script ELT: MongoDB para Azure Data Lake Storage

Aqui você encontrará a documentação do script Python responsável pela rotina de Extração, Carga e Transformação (ELT) de dados de um banco de dados MongoDB para o Azure Data Lake Storage. Este script é crucial para integrar dados NoSQL, que muitas vezes possuem esquemas flexíveis e aninhados, ao nosso ambiente de Data Lake estruturado, tornando-os acessíveis para análise e processamento subsequente.

A documentação detalha:
*   **Pré-requisitos:** Bibliotecas Python necessárias (e.g., `pymongo`, `azure-storage-blob`) e configurações de ambiente para conexão segura.
*   **Lógica de funcionamento:** Como o script se conecta ao MongoDB, extrai os dados (considerando grandes volumes e paginação), realiza transformações básicas (como normalização de JSON, inferência de esquema ou aplanamento de documentos aninhados) e os carrega no ADLS em formatos otimizados para análise (e.g., Parquet, Delta Lake).
*   **Variáveis de ambiente:** Parâmetros de configuração essenciais para a execução segura e flexível do script (e.g., connection strings, nomes de containers).

Este script garante que os dados NoSQL do nosso ambiente sejam ingeridos de forma eficiente e estruturada para o Data Lake, superando os desafios de integração de dados semi-estruturados.

[Acesse a documentação detalhada do script ELT MongoDB aqui.](mongo_script.md)

---

