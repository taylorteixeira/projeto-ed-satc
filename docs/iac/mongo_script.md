

# Script ELT: MongoDB para Azure Data Lake Storage

O script `elt_mongodb_n_collections.py` é responsável pela extração de dados de todas as coleções de um banco de dados MongoDB e pelo carregamento desses dados no Azure Data Lake Storage Gen2 (ADLS Gen2) em formato CSV. Ele atua como um componente de ingestão de dados fundamental, garantindo que as informações do MongoDB sejam disponibilizadas no data lake para posterior processamento, análise e consumo por outras ferramentas ou sistemas.

Este script segue uma abordagem ELT (Extract, Load, Transform):
1.  **Extract (Extração):** Conecta-se ao MongoDB e lê os documentos de cada coleção.
2.  **Load (Carga):** Carrega os dados brutos (ou minimamente processados) no ADLS Gen2.
3.  **Transform (Transformação):** Remove o campo `_id` padrão do MongoDB e converte os dados para CSV antes do carregamento. Esta transformação é realizada após a carga inicial dos dados brutos na memória, mas antes da escrita final no destino.

### 📜 Pré-requisitos

Para que o script funcione corretamente, as seguintes variáveis de ambiente devem ser configuradas. Recomenda-se a criação de um arquivo `.env` na raiz do projeto (dois níveis acima do script) para gerenciamento seguro e flexível dessas variáveis, evitando hardcoding de credenciais:

*   `MONGODB_URI`: URI de conexão com o MongoDB. Exemplos incluem:
    *   `mongodb://user:password@host:port/` para uma instância standalone.
    *   `mongodb+srv://user:password@cluster.mongodb.net/` para clusters MongoDB Atlas (SRV record).
    *   Certifique-se de que o usuário tem permissões de leitura para o banco de dados e coleções desejadas.
*   `MONGODB_DATABASE`: Nome do banco de dados MongoDB a ser processado.
*   `ADLS_ACCOUNT_NAME`: Nome da sua Storage Account do Azure Data Lake Storage Gen2 (ex: `mystorageaccount`).
*   `ADLS_FILE_SYSTEM_NAME`: Nome do File System (container) no ADLS Gen2 onde os dados serão carregados (ex: `raw-data`).
*   `ADLS_DIRECTORY_NAME`: O nome do diretório base dentro do File System onde os dados serão armazenados (ex: `mongodb_ingestion`).
*   `ADLS_SAS_TOKEN`: Token de Assinatura de Acesso Compartilhado (SAS) para o ADLS Gen2. Este token deve ter, no mínimo, permissões de `Write` e `Create` para o container e diretórios onde os dados serão salvos. Para ambientes de produção, é altamente recomendável usar Managed Identities ou Service Principals com RBAC do Azure para autenticação, em vez de SAS tokens, para maior segurança e gerenciamento centralizado.

Além disso, as seguintes bibliotecas Python devem ser instaladas:
*   `pymongo`: Driver oficial do MongoDB para Python.
*   `pandas`: Biblioteca para manipulação e análise de dados, usada para estruturar os documentos em DataFrames.
*   `azure-storage-file-datalake`: SDK do Azure para interagir com o ADLS Gen2.
*   `python-dotenv`: Facilita o carregamento de variáveis de ambiente de um arquivo `.env`.

### Detalhamento do Código

O script é dividido em seções lógicas para:

#### 1. Configuração de Logging e Carregamento de Variáveis de Ambiente

O script configura um sistema de logging robusto para acompanhar o progresso da execução, registrar eventos importantes e diagnosticar possíveis erros. Ele tenta carregar as variáveis de ambiente a partir de um arquivo `.env` localizado na raiz do projeto, tornando a configuração flexível e segura, evitando hardcoding de credenciais sensíveis. Caso o `.env` não seja encontrado, ele tenta carregar variáveis já definidas no ambiente do sistema.


#### 2. Conexão ao MongoDB

Utiliza a biblioteca `pymongo` para estabelecer uma conexão com o servidor MongoDB. Inclui uma validação de conexão (`client.admin.command('ping')`) para garantir que o banco de dados está acessível e que as credenciais estão corretas antes de prosseguir com a leitura dos dados. O `ServerApi("1")` é usado para garantir a compatibilidade com a API de versão 1 do MongoDB, o que ajuda a prevenir problemas de compatibilidade futura.


#### 3. Conexão ao Azure Data Lake Storage (ADLS Gen2)

Conecta-se ao ADLS Gen2 usando o token SAS para autenticação. Ele obtém um cliente para o sistema de arquivos especificado e cria um diretório com timestamp (`YYYYMMDD_HHMMSS`) dentro do `ADLS_DIRECTORY_NAME` configurado. Esta estratégia de timestamp é crucial para evitar sobrescrever dados de execuções anteriores, facilitando a rastreabilidade temporal e a recuperação de dados, além de permitir o processamento incremental ou histórico.


#### 4. Processamento e Carga de Coleções

O script itera sobre cada coleção encontrada no banco de dados MongoDB. Para cada coleção:
*   Lê todos os documentos usando `collection.find({})` e os carrega em um DataFrame do Pandas. Para coleções muito grandes, esta abordagem pode consumir muita memória; uma estratégia de paginação ou processamento em lotes seria mais eficiente.
*   Remove o campo `_id` (gerado automaticamente pelo MongoDB como um `ObjectId` BSON) do DataFrame. Este campo é primário no MongoDB, mas geralmente não é útil ou pode causar problemas de tipo de dados em formatos tabulares como CSV ou em sistemas de destino que esperam chaves primárias numéricas ou de string simples.
*   Verifica se o DataFrame resultante contém dados antes de prosseguir.
*   Converte o DataFrame para o formato CSV, garantindo que o índice do DataFrame não seja incluído (`index=False`) e codificando para UTF-8 para compatibilidade universal de caracteres.
*   Carrega o CSV resultante para o diretório timestamped no ADLS Gen2, nomeando o arquivo com o nome da coleção (`{collection_name}.csv`). A opção `overwrite=True` garante que, se por algum motivo o arquivo já existir (o que é improvável com o timestamp no diretório), ele será substituído.

#### 5. Tratamento de Erros e Finalização

O script utiliza blocos `try-except` aninhados e abrangentes para capturar e registrar erros em diferentes estágios: conexão com MongoDB, conexão com ADLS, autenticação e erros durante o processamento de coleções individuais. Isso garante que a execução seja robusta, que problemas sejam devidamente notificados via logs e que o script encerre com um código de saída apropriado (`sys.exit(0)` para sucesso, `sys.exit(1)` para falha). A conexão com o MongoDB é fechada no bloco `finally`, assegurando a liberação de recursos.

