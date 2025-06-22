---
hide:
  - navigation
  #- toc # table of contents - menu da direita
---

# Documentação do Projeto de Engenharia de Dados UniSATC

Bem-vindo à documentação oficial do projeto **Engenharia de Dados UniSATC**! Este projeto foi desenvolvido para demonstrar as funcionalidades e práticas de **Data Engineering** por meio da integração eficiente de infraestrutura, ingestão, processamento e análise de dados em larga escala. Aqui você encontrará uma visão completa do pipeline, da estrutura de arquivos a componentes detalhados.

---

## Estrutura da Documentação

### Infraestrutura como Código (IaC)

A infraestrutura do nosso projeto foi provisionada usando Terraform no Azure, destacando:
* **Azure Data Lake Storage Gen2 (ADLS):** Configurações para provisionar o Data Lake utilizado no pipeline.
* **Azure Databricks Workspace:** Detalhes sobre a criação de um ambiente Databricks empresarial, incluindo VNet Injection para segurança.

[Acesse a documentação de IaC aqui.](iac/index.md)

---

###  Projeto Principal (`projeto_ed_satc`)

O núcleo do nosso projeto é o pacote Python `projeto_ed_satc`. Ele encapsula a lógica para integrar MongoDB com ADLS e também contém os **Notebooks Databricks** que implementam as camadas do pipeline **Lakehouse (Bronze, Silver, Gold)**.

[Acesse a documentação do pacote principal aqui.](projeto_ed_satc/index.md)

---



