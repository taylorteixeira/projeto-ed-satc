import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import ConnectionFailure, OperationFailure
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError, ClientAuthenticationError

import os
import sys
from dotenv import load_dotenv
from pathlib import Path
import logging
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Caminho para o arquivo .env na raiz do projeto (dois níveis acima)
project_root = Path(__file__).resolve().parent.parent
env_path = project_root / ".env"

logger.info(f"Tentando carregar .env de: {env_path}")
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
    logger.info(f"DEBUG: .env carregado de {env_path}")
else:
    logger.warning(f"ALERTA: Arquivo .env não encontrado em {env_path}. Tentando carregar automaticamente.")
    if not load_dotenv(override=True):
        logger.error("DEBUG: Nenhum arquivo .env encontrado automaticamente.")
        sys.exit(1)

# Configurações do MongoDB
mongo_uri = os.getenv("MONGODB_URI")
database_name = os.getenv("MONGODB_DATABASE")

# Configurações do Azure Data Lake Storage
account_name = os.getenv("ADLS_ACCOUNT_NAME")
file_system_name = os.getenv("ADLS_FILE_SYSTEM_NAME")
directory_name = os.getenv("ADLS_DIRECTORY_NAME", database_name)  # Usa ADLS_DIRECTORY_NAME se definido, senão usa o nome do banco
sas_token = os.getenv("ADLS_SAS_TOKEN")

# Verificar se todas as variáveis necessárias foram carregadas
required_vars = {
    "MONGODB_URI": mongo_uri,
    "MONGODB_DATABASE": database_name,
    "ADLS_ACCOUNT_NAME": account_name,
    "ADLS_FILE_SYSTEM_NAME": file_system_name,
    "ADLS_SAS_TOKEN": sas_token
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    logger.error(f"Variáveis de ambiente ausentes: {', '.join(missing_vars)}")
    sys.exit(1)

try:
    # Conectar ao MongoDB
    logger.info(f"Conectando ao MongoDB (banco de dados: {database_name})...")
    client = MongoClient(mongo_uri, server_api=ServerApi("1"), serverSelectionTimeoutMS=5000)
    
    # Verificar conexão MongoDB
    client.admin.command('ping')
    logger.info("Conexão com MongoDB estabelecida com sucesso")
    
    db = client[database_name]
    
    # Listar todas as coleções do banco de dados
    collections = db.list_collection_names()
    logger.info(f"Coleções encontradas no banco de dados: {len(collections)}")
    if not collections:
        logger.warning(f"Nenhuma coleção encontrada no banco de dados '{database_name}'")
    
    # Criar cliente do Azure Data Lake Storage
    logger.info(f"Conectando ao Azure Data Lake Storage (conta: {account_name})...")
    try:
        service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=sas_token
        )
        
        # Obter referência ao sistema de arquivos
        file_system_client = service_client.get_file_system_client(file_system_name)
        
        # Verificar se o sistema de arquivos existe
        try:
            file_system_client.get_file_system_properties()
            logger.info(f"Sistema de arquivos '{file_system_name}' acessado com sucesso")
        except ResourceNotFoundError:
            logger.error(f"Sistema de arquivos '{file_system_name}' não encontrado")
            sys.exit(1)
        
        # Criar diretório com timestamp para evitar sobrescrever dados antigos
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        directory_path = f"{directory_name}/{timestamp}"
        
        try:
            directory_client = file_system_client.get_directory_client(directory_path)
            directory_client.create_directory()
            logger.info(f"Diretório '{directory_path}' criado com sucesso")
        except ResourceExistsError:
            logger.info(f"Diretório '{directory_path}' já existe")
        
        # Para cada coleção encontrada, ler os dados e carregar para o Azure Data Lake Storage
        successful_collections = 0
        failed_collections = 0
        
        for collection_name in collections:
            try:
                logger.info(f"Processando coleção: {collection_name}")
                collection = db[collection_name]
                
                # Contar documentos para logging e verificação
                doc_count = collection.count_documents({})
                logger.info(f"Coleção '{collection_name}' contém {doc_count} documentos")
                
                if doc_count == 0:
                    logger.warning(f"Coleção '{collection_name}' está vazia. Continuando para a próxima coleção.")
                    continue
                
                # Ler todos os documentos da coleção em um DataFrame
                cursor = collection.find({})
                df = pd.DataFrame(list(cursor))
                
                # Remover o campo _id do MongoDB que pode causar erros na serialização
                if '_id' in df.columns:
                    df = df.drop('_id', axis=1)
                
                # Verificar se temos dados para salvar
                if df.empty:
                    logger.warning(f"Nenhum dado válido encontrado na coleção '{collection_name}'. Continuando para a próxima.")
                    continue
                
                # Carregar os dados para o Azure Data Lake Storage
                file_client = directory_client.get_file_client(f"{collection_name}.csv")
                data = df.to_csv(index=False).encode('utf-8')
                file_client.upload_data(data, overwrite=True)
                
                logger.info(f"Dados da coleção '{collection_name}' carregados com sucesso ({len(df)} registros)")
                successful_collections += 1
                
            except Exception as e:
                logger.error(f"Erro ao processar coleção '{collection_name}': {str(e)}")
                failed_collections += 1
        
        # Resumo da operação
        logger.info(f"Migração concluída: {successful_collections} coleções migradas com sucesso, {failed_collections} com falha")
        
    except ClientAuthenticationError:
        logger.error("Falha na autenticação com o Azure Data Lake. Verifique o token SAS.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro ao conectar ao Azure Data Lake Storage: {str(e)}")
        sys.exit(1)

except ConnectionFailure as e:
    logger.error(f"Não foi possível conectar ao MongoDB: {str(e)}")
    sys.exit(1)
except OperationFailure as e:
    if "Authentication failed" in str(e):
        logger.error("Falha na autenticação do MongoDB. Verifique usuário e senha.")
    else:
        logger.error(f"Erro de operação do MongoDB: {str(e)}")
    sys.exit(1)
except Exception as e:
    logger.error(f"Erro inesperado: {str(e)}")
    sys.exit(1)
finally:
    if 'client' in locals():
        client.close()
        logger.info("Conexão com MongoDB fechada")

logger.info("Script de migração concluído com sucesso!")