# Projeto de Engenharia de Dados UniSATC (`projeto_ed_satc`)

Bem-vindo à documentação do pacote Python `projeto_ed_satc`! Este pacote constitui o coração do nosso projeto de engenharia de dados, encapsulando a lógica central para ingestão, processamento e transformação de dados, bem como a orquestração de pipelines. Ele foi desenvolvido para ser modular, eficiente e escalável, utilizando tecnologias como PySpark (via Databricks) e integração com o Azure Data Lake Storage.

Aqui você encontrará detalhes sobre os principais componentes de código que impulsionam o pipeline de dados:

---

## ⚡ Script ELT: MongoDB para Azure Data Lake Storage

Esta seção documenta o script Python (`elt_mongodb_n_collections.py`) responsável pela rotina de Extração, Carga e Transformação (ELT) de dados de um banco de dados MongoDB para o Azure Data Lake Storage Gen2 (ADLS Gen2). Este script é crucial para integrar dados NoSQL, que muitas vezes possuem esquemas flexíveis e aninhados, ao nosso ambiente de Data Lake estruturado, tornando-os acessíveis para análise e processamento subsequente por ferramentas como Databricks ou Power BI.

A documentação detalha:
*   **Pré-requisitos:** Bibliotecas Python necessárias e configurações de ambiente para conexão segura.
*   **Lógica de funcionamento:** Como o script se conecta ao MongoDB, extrai os dados, realiza transformações básicas e os carrega no ADLS em formatos otimizados para análise.
*   **Variáveis de ambiente:** Parâmetros de configuração essenciais para a execução segura e flexível do script.

Este script garante que os dados NoSQL do nosso ambiente sejam ingeridos de forma eficiente e estruturada para o Data Lake, superando os desafios de integração de dados semi-estruturados e não relacionais.

[Acesse a documentação detalhada do script ELT MongoDB aqui.](mongo_script.md)

---

## ☁️ Notebooks Databricks: Pipeline de Dados Lakehouse

Esta seção abrange a documentação dos notebooks PySpark executados no ambiente Databricks, que implementam as camadas do nosso pipeline de dados Lakehouse. O Databricks, com o Delta Lake como formato de armazenamento padrão, permite a construção de uma arquitetura Lakehouse que combina a flexibilidade e escalabilidade de um Data Lake com a confiabilidade, performance e recursos de transação (ACID) de um Data Warehouse tradicional. Isso inclui operações como `UPSERT`, `DELETE`, `MERGE` e `time travel`.

Explicamos o papel de cada notebook (`Bronze`, `Silver`, `Gold`) na transformação e curadoria dos dados:
*   **Bronze Layer:** Responsável pela ingestão de dados brutos (`raw data`) de diversas fontes.
*   **Silver Layer:** Foca no refinamento e limpeza dos dados, aplicando transformações e validações.
*   **Gold Layer:** Camada de curadoria e agregação, onde os dados são transformados em modelos de dados otimizados para consumo analítico.

É aqui que a magia do processamento de dados acontece, transformando dados brutos em insights valiosos e estruturados, prontos para serem consumidos por aplicações e usuários de negócios, aproveitando a capacidade de processamento distribuído do Apache Spark no Databricks.

[Acesse a documentação detalhada dos Notebooks Databricks aqui.](databricks_notebooks.md)