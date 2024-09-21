import logging
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from dotenv import load_dotenv
import os
import requests

# Конфигурация логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Загружаем переменные из .env файла
load_dotenv()

# Глобальные переменные для хранения сессий Salesforce
sf_rest_session = None
sf_bulk_session = None

def authenticate_salesforce(api_type='both'):
    try:
        # Определяем, в каком окружении мы работаем (test, dev, prod)
        environment = os.getenv('ENVIRONMENT')

        if environment == 'prod':
            username = os.getenv('PROD_SALESFORCE_USERNAME')
            password = os.getenv('PROD_SALESFORCE_PASSWORD')
            security_token = os.getenv('PROD_SALESFORCE_TOKEN')
            domain = os.getenv('PROD_SALESFORCE_DOMAIN')
            logger.info("Используем продакшен окружение.")
        
        elif environment == 'dev':
            username = os.getenv('DEVSF_USERNAME')
            password = os.getenv('DEVSF_PASSWORD')
            security_token = os.getenv('DEVSF_TOKEN')
            domain = os.getenv('DEVSF_DOMAIN')
            logger.info("Используем dev окружение.")
        
        else:
            username = os.getenv('SALESFORCE_USERNAME')
            password = os.getenv('SALESFORCE_PASSWORD')
            security_token = os.getenv('SALESFORCE_TOKEN')
            domain = os.getenv('SALESFORCE_DOMAIN')
            logger.info("Используем тестовое окружение.")

        # Авторизация через simple_salesforce (REST API)
        sf = Salesforce(username=username, password=password, security_token=security_token, domain=domain)

        # Определяем, какой тип API нужен
        if api_type == 'sf' or api_type == 'both':
            logger.info("Успешная авторизация в Salesforce (REST API)")
            return sf

        # Авторизация для Bulk API
        if api_type == 'bulk' or api_type == 'both':
            bulk = SalesforceBulk(sessionId=sf.session_id, host=sf.sf_instance)
            logger.info("Успешная авторизация в Salesforce Bulk API")
            return bulk

        if api_type == 'both':
            return sf, bulk

    except Exception as e:
        logger.error(f"Ошибка авторизации: {e}")
        return None

def initialize_salesforce_session():
    """
    Авторизуемся в Salesforce и сохраняем сессии для REST API и Bulk API для последующего использования.
    """
    global sf_rest_session, sf_bulk_session
    if not sf_rest_session or not sf_bulk_session:
        sessions = authenticate_salesforce(api_type='both')
        if isinstance(sessions, tuple):
            sf_rest_session, sf_bulk_session = sessions
            logger.info("Salesforce sessions initialized for both REST and Bulk API.")
        elif sessions:
            sf_rest_session = sessions
            logger.info("Salesforce session initialized for REST API.")
        else:
            logger.error("Failed to authenticate with Salesforce.")

    return sf_rest_session, sf_bulk_session

def download_and_save_file(content_document_id, save_folder):
    """
    Загружает файл с указанным ContentDocumentId через REST API Salesforce и сохраняет его в указанную папку.
    
    :param content_document_id: ID документа в Salesforce (ContentDocumentId)
    :param save_folder: Путь для сохранения загруженного файла
    :return: Путь к сохраненному файлу или ошибка
    """
    try:
        global sf_rest_session

        if not sf_rest_session:
            raise Exception('Salesforce REST session not initialized')

        # Формируем URL для загрузки файла по ContentDocumentId
        download_url = f"https://{sf_rest_session.sf_instance}/services/data/vXX.X/sobjects/ContentDocument/{content_document_id}/VersionData"
        
        # Отправляем запрос на загрузку файла
        headers = {'Authorization': f'Bearer {sf_rest_session.session_id}'}
        response = requests.get(download_url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Error downloading file: {response.content}")

        # Определяем путь для сохранения файла и сохраняем его
        if not os.path.exists(save_folder):
            os.makedirs(save_folder)

        file_path = os.path.join(save_folder, f"{content_document_id}.xlsx")
        with open(file_path, 'wb') as file:
            file.write(response.content)

        logger.info(f"File successfully downloaded and saved at {file_path}")
        return file_path

    except Exception as e:
        logger.error(f"Error occurred during file download: {str(e)}")
        return None
