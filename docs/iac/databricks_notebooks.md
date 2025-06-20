# Notebooks Databricks: Pipeline de Dados Lakehouse

Esta seção detalha os notebooks PySpark executados no ambiente Databricks, que formam a espinha dorsal do nosso pipeline de Engenharia de Dados. Eles são responsáveis por processar os dados brutos desde sua ingestão até a camada de consumo, seguindo o padrão Lakehouse com as camadas Bronze, Silver e Gold. Este modelo aproveita o Delta Lake para garantir transações ACID, schema enforcement, escalabilidade e recursos de time travel, essenciais para um ambiente de dados robusto e confiável.

Cada notebook é projetado para uma fase específica do processamento de dados, garantindo modularidade, rastreabilidade, idempotência e alta qualidade dos dados.

## 1. Notebook Bronze (`bronze.ipynb`)

O notebook `bronze.ipynb` é a primeira etapa do pipeline de dados, atuando como o ponto de entrada para os dados brutos. Sua principal função é ingerir os dados brutos (CSV, neste caso) da **Landing Zone** do Azure Data Lake Storage Gen2 (ADLS Gen2) e persistí-los na camada **Bronze** em formato Delta Lake.

Nesta camada, os dados são armazenados de forma quase "raw", com poucas ou nenhuma transformação, mantendo a fidelidade aos dados de origem. A ideia é criar um "espelho" dos dados de origem, permitindo que futuras transformações ou correções de erros possam ser feitas a partir do dado original. São adicionadas colunas de metadados como `data_hora_bronze` (timestamp de ingestão) e `nome_arquivo` (para rastreabilidade da origem e auditoria).

### Principais Funcionalidades:

*   **Montagem do ADLS Gen2:** Estabelece a conexão segura com os containers do ADLS Gen2 (`landing-zone` para leitura e `bronze` para escrita). A montagem é uma prática recomendada para simplificar o acesso aos dados no Databricks.
*   **Leitura de Dados Brutos:** Lê arquivos CSV da `landing-zone`. É crucial considerar opções de leitura como `header`, `inferSchema` (para prototipagem) ou `schema` (para produção, garantindo consistência), e tratamento de dados malformados (`badRecordsPath` ou `mode` como `PERMISSIVE`).
*   **Adição de Metadados:** Inclui timestamps de processamento (`current_timestamp()`) e o nome do arquivo de origem (`input_file_name()`), que são fundamentais para linhagem de dados, depuração e reprocessamento.
*   **Persistência para Bronze:** Escreve os dados no formato Delta Lake na camada `bronze` do ADLS Gen2. A organização dos dados por tabelas (ex: `/bronze/assistencias`) dentro do Delta Lake permite o uso de recursos como `MERGE INTO` para upserts e `OPTIMIZE` para compactação de arquivos, melhorando a performance de leitura.

\
`src/iac/databricks/bronze.ipynb`


---

## 2. Notebook Silver (`silver.ipynb`)

O notebook `silver.ipynb` representa a segunda fase do pipeline, focando na curadoria e padronização dos dados. Ele consome os dados da camada **Bronze** (formato Delta Lake), aplica um conjunto de transformações de limpeza e padronização e, em seguida, persiste os dados na camada **Silver**, também em formato Delta Lake.

A camada Silver contém dados limpos, consistentes e estruturados, prontos para análises mais aprofundadas ou para alimentar a camada Gold. Nesta etapa, são realizadas operações críticas como renomeação e padronização de colunas, remoção de dados redundantes (deduplicação), tratamento de valores nulos, correção de tipos de dados e aplicação de regras de negócio básicas. O objetivo é garantir a qualidade e a conformidade dos dados antes que sejam usados para consumo.

### Principais Funcionalidades:

*   **Montagem do ADLS Gen2:** Garante acesso seguro aos containers `bronze` (para leitura) e `silver` (para escrita).
*   **Leitura de Dados Bronze:** Carrega os dados Delta da camada `bronze`. Para pipelines incrementais, é comum usar `readStream` com `option("maxFilesPerTrigger", 1)` ou `option("maxBytesPerTrigger", "10g")` para processar novos dados de forma eficiente.
*   **Adição de Metadados Silver:** Inclui timestamps de processamento `data_hora_silver` e o nome do arquivo de origem na camada Silver, mantendo a rastreabilidade e o histórico de processamento.
*   **Padronização de Colunas (`renomear_colunas`):** Esta função é crucial para a consistência dos dados. Ela padroniza nomes de colunas (ex: `CD_` para `CODIGO_`, `DT_` para `DATA_`, etc.) e remove metadados da camada anterior que não são mais relevantes para a camada Silver.

*   **Persistência para Silver:** Salva os DataFrames processados na camada `silver` do ADLS Gen2 no formato Delta Lake. A estratégia de escrita (ex: `overwrite`, `append`, `merge`) deve ser cuidadosamente escolhida com base na idempotência do processo e na lógica de atualização dos dados. Para tabelas que sofrem alterações incrementais, o `MERGE INTO` é altamente recomendado para upserts eficientes. A otimização com `ZORDER BY` em colunas frequentemente usadas em filtros pode melhorar significativamente a performance de leitura.

\
`src/iac/databricks/silver.ipynb`


---

## 3. Notebook Gold (`gold.ipynb`)

O notebook `gold.ipynb` é a etapa final e mais refinada do pipeline, projetada para o consumo direto por usuários de negócio, ferramentas de BI, cientistas de dados e aplicações. Ele consome os dados limpos e transformados da camada **Silver** (formato Delta Lake) e os remodela em tabelas dimensionais (Dims) e tabelas de fatos (Fatos), adequadas para análise de negócio, BI e Machine Learning.

A camada Gold é otimizada para o consumo, com dados agregados e estruturados em um esquema que facilita a consulta e a compreensão pelos usuários de negócio. A modelagem dimensional (star ou snowflake schema) é a abordagem padrão aqui, pois simplifica consultas complexas e melhora o desempenho. Além da modelagem, este notebook demonstra a criação de métricas e KPIs importantes, que são pré-calculados para agilizar o acesso à informação.

### Principais Funcionalidades:

*   **Montagem do ADLS Gen2:** Conecta-se ao container `gold` para escrita das tabelas finais.
*   **Leitura de Dados Silver:** Carrega os dados Delta da camada `silver`. É comum ler várias tabelas Silver para construir as dimensões e fatos.
*   **Criação de Schema Gold:** Garante que o schema `gold` exista no ambiente Databricks para organizar as tabelas finais. Isso é crucial para a governança de dados e para a organização do catálogo de dados.
*   **Modelagem Dimensional:**
    *   **`dim_usuario`:** Cria uma dimensão de usuários, calculando a idade atual. Dimensões devem conter atributos descritivos e estáveis sobre as entidades de negócio.
    *   **`dim_plano`:** Cria uma dimensão para os planos de assinatura.
    *   **`dim_conteudo`:** Unifica informações de filmes e episódios de séries em uma única dimensão de conteúdo, demonstrando a consolidação de dados para uma visão unificada.
    *   **`dim_tempo`:** Gera uma dimensão de tempo abrangendo um período definido, útil para análises temporais. Dimensões de tempo são essenciais para qualquer modelo de dados, permitindo análises por dia, semana, mês, ano, etc.
*   **Criação da Tabela de Fatos:**
    *   **`fato_resumo_usuario_mensal`:** Agrega métricas mensais por usuário, combinando pagamentos, assistências e avaliações. Inclui flags de atividade. Tabelas de fatos contêm métricas numéricas e chaves estrangeiras para as dimensões, permitindo análises multidimensionais. O "grain" (granularidade) desta tabela é mensal por usuário.
*   **Criação de KPIs e Métricas de Negócio (Views Temporárias):** A criação de views temporárias ou tabelas agregadas é uma prática comum para pré-computar métricas complexas, melhorando o desempenho das consultas de BI.
    *   `metrica_minutos_assistidos`: Total de minutos assistidos por mês.
    *   `metrica_novos_usuarios`: Contagem de novos usuários por mês de cadastro.
    *   `kpi_mrr`: Monthly Recurring Revenue (Receita Recorrente Mensal) - uma métrica financeira chave.
    *   `kpi_churn_rate`: Taxa de evasão de usuários - importante para retenção.
    *   `kpi_ltv`: Lifetime Value (Valor do Tempo de Vida do Cliente) - valor total que um cliente gera para a empresa.
    *   `kpi_engajamento_pais_plano`: Média de minutos assistidos por país e plano.
*   **Persistência para Gold:** Salva as tabelas dimensionais e de fatos na camada `gold` do ADLS Gen2 e as registra como tabelas Delta no Databricks. É fundamental usar `CREATE OR REPLACE TABLE` para garantir a idempotência do processo e facilitar o reprocessamento. A criação de tabelas Delta gerenciadas ou externas no Databricks permite que elas sejam facilmente descobertas e consultadas via SQL por ferramentas de BI.

\
`src/iac/databricks/gold.ipynb`
