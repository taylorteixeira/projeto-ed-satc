# Dashboard e KPIs do Projeto

Este documento apresenta informações sobre as visualizações criadas no **Databricks** para dados importantes relacionados ao comportamento dos usuários e ao desempenho do negócio.

---

## Estrutura do Dashboard

O dashboard contém visualizações para análise de engajamento, taxa de cancelamento e correlações entre métricas críticas. Abaixo estão as principais informações sobre as visualizações:

### 1. **Média de Minutos Assistidos por País e Plano**

- **Tipo de Gráfico:** Linha
- **Fonte de Dados:** `gold.kpi_engajamento_pais_plano`
- **Descrição:**
  - Este gráfico analisa a média de minutos assistidos por mês por país e plano.
  - Ajuda a visualizar as tendências de engajamento por regiões e tipos de planos ao longo do tempo.

---

### 2. **Variação da Taxa de Cancelamento ao Longo do Tempo**

- **Tipo de Gráfico:** Linha
- **Fonte de Dados:** `gold.kpi_churn_rate`
- **Descrição:**
  - Monitora a mudança na taxa de cancelamento de usuários (churn rate) ao longo do tempo.
  - Identifica meses onde as taxas subiram ou caíram, permitindo análise e planejamento para retenção.

---

### 3. **Correlação entre Média de Minutos Assistidos e Taxa de Cancelamento**

- **Tipo de Gráfico:** Dispersão
- **Fonte de Dados:** `gold.kpi_engajamento_pais_plano` e `gold.kpi_churn_rate`
- **Descrição:**
  - Avalia a relação entre o tempo de engajamento dos usuários (minutos assistidos) e a taxa de cancelamento.
  - Permite identificar se maior engajamento influencia positivamente na retenção.

---

### 4. **Taxa de Cancelamento por País**

- **Tipo de Gráfico:** Barras
- **Fonte de Dados:** `gold.kpi_engajamento_pais_plano` e `gold.kpi_churn_rate`
- **Descrição:**
  - Compara as taxas de cancelamento entre diferentes países.
  - Oferece insights sobre regiões onde os usuários cancelam mais frequentemente.

---

## KPIs e Métricas no Databricks

As visualizações mencionadas acima são derivadas de **views** SQL criadas na camada **gold** do Data Lake. Abaixo estão as principais **views** utilizadas:

### 1. View: `gold.kpi_mrr`

- **Descrição:** Calcula o **MRR (Monthly Recurring Revenue)** somando os valores pagos mensalmente.

---

### 2. View: `gold.kpi_churn_rate`

- **Descrição:** Calcula a taxa de cancelamento percentual dos usuários mês a mês.

---

### 3. View: `gold.kpi_engajamento_pais_plano`

- **Descrição:** Analisa o engajamento médio dos usuários com base nas métricas de minutos assistidos agrupados por mês, país e planos.

---

### 4. **`gold.kpi_ltv`:**

- **Descrição:** Estima o **LTV (Lifetime Value)** com base no ARPU (Average Revenue Per Unit) e taxa de churn.

---

### 5. **`gold.metrica_novos_usuarios`:**

- **Descrição:** Mede novos usuários cadastrados por mês.

---

### 6. **`gold.metrica_minutos_assistidos`:**

- **Descrição:** Mede o total de minutos assistidos por mês.

---
