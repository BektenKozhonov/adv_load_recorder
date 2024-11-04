import logging
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from dotenv import load_dotenv
import os
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Загружаем переменные из .env файла
load_dotenv()


def initialize_salesforce_session():
    """Авторизуемся в Salesforce и сохраняем сессии для REST API и Bulk API для последующего использования."""
    try:
        environment = os.getenv('ENVIRONMENT')
        if environment == 'prod':
            username = os.getenv('PROD_SALESFORCE_USERNAME')
            password = os.getenv('PROD_SALESFORCE_PASSWORD')
            security_token = os.getenv('PROD_SALESFORCE_TOKEN')
            domain = os.getenv('PROD_SALESFORCE_DOMAIN')
        elif environment == 'dev':
            username = os.getenv('DEVSF_USERNAME')
            password = os.getenv('DEVSF_PASSWORD')
            security_token = os.getenv('DEVSF_TOKEN')
            domain = os.getenv('DEVSF_DOMAIN')
        else:
            username = os.getenv('SALESFORCE_USERNAME')
            password = os.getenv('SALESFORCE_PASSWORD')
            security_token = os.getenv('SALESFORCE_TOKEN')
            domain = os.getenv('SALESFORCE_DOMAIN')

        sf = Salesforce(username=username, password=password, security_token=security_token, domain=domain)
        bulk = SalesforceBulk(sessionId=sf.session_id, host=sf.sf_instance)
        logger.info("Successfully authenticated to Salesforce.")
        return sf, bulk
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        return None, None


def download_and_save_file(content_document_id, save_folder):
    """Загружает файл с указанным ContentDocumentId через REST API Salesforce и сохраняет его в указанную папку."""
    try:
        sf_rest_session, _ = initialize_salesforce_session()
        if not sf_rest_session:
            raise Exception(f'Salesforce REST session not initialized')

        download_url = f"https://{sf_rest_session.sf_instance}/services/data/v61.0/sobjects/ContentDocument/{content_document_id}/VersionData"
        headers = {'Authorization': f'Bearer {sf_rest_session.session_id}'}
        response = requests.get(download_url, headers=headers)

        if response.status_code != 200:
            raise Exception(f"Error downloading file: {response.content}")

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
