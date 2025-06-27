# 📊 Dashboard de KPIs - Databricks

Este projeto contém os scripts SQL e o template JSON do dashboard de KPIs operacionais da plataforma, como **MRR**, **Churn**, **ARPU**, entre outros. Ele foi desenvolvido e é executado no ambiente do [Azure Databricks].

---

## 🧩 Conteúdo

- `camposDashboard.sql`: query SQL que gera todos os dados utilizados no dashboard.
- `dashboard.json`: exportação do dashboard em formato JSON para recriação automática via API.
- `README.md`: instruções detalhadas de uso.

---

## 🚀 Como Criar o Dashboard no Databricks

### 1. Importar Query SQL

1. Acesse o [Databricks SQL Editor](https://<seu-workspace>.azuredatabricks.net/sql/editor).
2. Crie um novo script SQL.
3. Copie o conteúdo do arquivo `camposDashboard.sql` e cole no editor.
4. Execute a query (`▶️`).

---

### 2. Criar Visualizações

1. Após rodar a query, clique em **“+ Visualização”**.
2. Crie gráficos como:
   - Tabela (resultados brutos)
   - Linhas para `MRR`, `ARPU`, `Churn`, etc.
   - Barras para `Novos Usuários` e `Minutos Assistidos`
3. Dê nomes apropriados às visualizações.
4. Salve cada uma.

---

### 3. Criar Dashboard

1. Vá em **SQL > Dashboards**.
2. Clique em **“+ Criar Dashboard”**.
3. Nomeie como `Campos para Dashboard`.
4. Clique em **+ Adicionar Visualização** e selecione as visualizações criadas.
5. Organize os componentes no layout desejado (drag-and-drop).
6. Salve o dashboard.

---

## 💾 Como Exportar o Dashboard (Para Backup ou Versionamento)

1. No painel de dashboards, clique nos `...` > **Exportar**.
2. Escolha a opção **Exportar JSON**.
3. Salve esse conteúdo no arquivo `dashboard.json`.

> Esse arquivo pode ser reutilizado após a destruição do ambiente com Terraform, garantindo reprodutibilidade.

---

## 🔁 Como Restaurar o Dashboard após `terraform destroy`

### A. Manualmente

1. Recrie o ambiente (`terraform apply`).
2. Importe o conteúdo de `camposDashboard.sql` no SQL Editor.
3. Recrie visualizações e o dashboard manualmente.

### B. Automático (via API)

Você pode usar a [SQL Dashboard API v2.0](https://docs.databricks.com/api/sql/dashboards.html) para importar o JSON:

```bash
curl -X POST https://<seu-workspace>.azuredatabricks.net/api/2.0/sql/dashboards/import \
  -H "Authorization: Bearer <seu-token>" \
  -H "Content-Type: application/json" \
  -d @dashboard.json
