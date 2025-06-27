# Script ELT: MongoDB para Azure Data Lake Storage

O arquivo de notebook **injector** é responsável pela extração de dados de coleções em um banco de dados MongoDB e sua posterior carga no **Azure Data Lake Storage Gen2 (ADLS Gen2)** em formato CSV. Este processo segue a abordagem ELT (Extract, Load, Transform), viabilizando a disponibilização de dados estruturados no Data Lake para processamento, análise ou uso por outras ferramentas.

---

## Estrutura ELT Implementada

1. **Extract (Extração):**
   - Estabelece conexão com o MongoDB.
   - Obtém documentos de todas as coleções disponíveis em um banco de dados específico.

2. **Load (Carga):**
   - Os documentos são carregados no ADLS Gen2 em formato CSV.
   - Cada coleção é salva como um arquivo `.csv` em um diretório com timestamp para rastreabilidade.

3. **Transform (Transformação):**
   - O campo `_id` (identificador padrão do MongoDB) é removido.
   - Os dados são transformados para um formato amigável (CSV) para interoperabilidade com outras ferramentas.

---

## Pré-Requisitos

1. **Configuração de Widgets no Databricks:**
### - Utiliza **widgets** para capturar informações configuráveis necessárias durante a execução. Eles incluem:

| - **Banco de dados MongoDB:** Nome do banco de dados a ser processado. |
|---|


     - **Descrição da conta no ADLS Gen2:** Nome da conta de armazenamento, sistema de arquivos e diretório destino.
     - **Credenciais de autenticação:** String de conexão com o MongoDB e token SAS para o ADLS.

2. **Bibliotecas Necessárias:**
### O script exige as seguintes bibliotecas Python, instaladas previamente no ambiente:

| - **pymongo:** Para conexão e manipulação do MongoDB. |
|---|


   - **azure-storage-file-datalake:** Para interação com o Azure Data Lake.
   - **pandas:** Para processamento e estruturação dos dados.
   - Comando utilizado no notebook para instalar essas bibliotecas:
     ```python
     %pip install pymongo azure-storage-file-datalake pandas
     ```

3. **Credenciais Necessárias:**
   - **MongoDB URI:** String de conexão para acesso ao banco de dados.
   - **Token SAS do Azure Data Lake:** Especifica permissões de acesso (leitura e escrita) ao container/diretório.

---

## Detalhamento do Código

### 1. Configuração

O script inicializa capturando os parâmetros configuráveis via **widgets**. Esses parâmetros permitem a flexibilidade de ajustar o comportamento do script sem alteração no código.

**Exemplo de widgets utilizados:**
```python
dbutils.widgets.text("mongodb_database", "projetoaws", "2. Nome do Banco de Dados MongoDB")
dbutils.widgets.text("adls_account_name", "datalakef77be278f4c3d227", "3. Nome da Conta de Storage (ADLS)")
dbutils.widgets.text("adls_file_system_name", "landing-zone", "4. Nome do File System (Contêiner)")
dbutils.widgets.text("adls_directory_name", "dados", "5. Diretório Base de Destino no ADLS")
dbutils.widgets.text("mongo_uri", "mongodb+srv://root:senha@cluster...", "6. Nome da Chave do Segredo (MongoDB URI)")
dbutils.widgets.text("sas_token", "token_sas_gerado...", "7. Nome da Chave do Segredo (ADLS SAS Token)")
```

### 2. Conexão com o MongoDB

### A conexão com o MongoDB é estabelecida utilizando a biblioteca `pymongo`. O script:

| - Verifica a acessibilidade do banco de dados utilizando o comando `ping`. |
|---|


- Obtém uma lista de coleções disponíveis para processamento.

**Validação:**
- Caso nenhuma coleção seja encontrada, o script encerra com uma mensagem apropriada.

**Trecho de exemplo:**
```python
from pymongo import MongoClient
from pymongo.server_api import ServerApi

mongo_client = MongoClient(mongo_uri, server_api=ServerApi("1"), serverSelectionTimeoutMS=5000)
mongo_client.admin.command('ping')
db = mongo_client[mongodb_database]
collections = db.list_collection_names()
```

### 3. Conexão ao ADLS Gen2

O script conecta-se ao Azure Data Lake utilizando `azure-storage-file-datalake`. Um diretório com timestamp é criado no container especificado para evitar sobreposição de dados de execuções anteriores.

**Processo:**
- Um **DataLakeServiceClient** é instanciado com a URL da conta e o token SAS.
- Verificações são realizadas para garantir que o *file system* e o diretório estão devidamente configurados.

**Trecho de exemplo:**
```python
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime

service_client = DataLakeServiceClient(
    account_url=f"https://{adls_account_name}.dfs.core.windows.net",
    credential=sas_token
)
file_system_client = service_client.get_file_system_client(file_system_name)
directory_path = f"{directory_name}/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
directory_client = file_system_client.get_directory_client(directory_path)
directory_client.create_directory()
```

### 4. Processamento de Dados e Upload para o ADLS

### Cada coleção do MongoDB é processada individualmente:

| 1. Dados são extraídos da coleção e carregados em um `DataFrame` do Pandas. |
|---|


2. O campo `_id` é removido para evitar problemas de compatibilidade.
3. O DataFrame é convertido em arquivo CSV.
4. O arquivo CSV é carregado no diretório configurado.

**Trecho de exemplo:**
```python
for collection_name in collections:
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)
    csv_data = df.to_csv(index=False, encoding='utf-8')
    file_client = directory_client.get_file_client(f"{collection_name}.csv")
    file_client.upload_data(csv_data.encode('utf-8'), overwrite=True)
```

### 5. Tratamento de Erros

### O script utiliza blocos `try-except` para capturar erros em diferentes estágios:

| - Conexão ao MongoDB e autenticação. |
|---|


- Acesso ao Azure Data Lake.
- Erros durante o processamento de coleções.

Se um erro crítico ocorrer, a execução é encerrada com uma mensagem de erro apropriada.

**Tratamento de erro para conexão MongoDB:**
```python
except ConnectionFailure as e:
    dbutils.notebook.exit(f"FALHA CRÍTICA: Não foi possível conectar ao MongoDB: {e}")
```

---

## Resumo da Operação

### Ao final da execução, o script exibe um resumo com:

| - Número de coleções processadas com sucesso. |
|---|


- Número de coleções com falha.

**Trecho de saída esperada:**
```
--- Resumo da Operação ---
Migração concluída.
Coleções migradas com sucesso: 3
Coleções com falha: 1
```

\
`src/projeto_ed_satc/pipeline/injector/injector.ipynb`