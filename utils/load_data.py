import logging
import pandas as pd
from io import StringIO
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from dotenv import load_dotenv
import os
import requests
from typing import Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Загружаем переменные из .env файла
load_dotenv()


class SalesforceAuthentication:
    # Атрибуты класса для хранения сессий, общих для всех экземпляров
    sf_rest_session = None
    sf_bulk_session = None

    @classmethod
    def initialize_salesforce_session(cls):
        """Авторизуемся в Salesforce и сохраняем сессии для REST API и Bulk API для последующего использования."""
        try:
            username = os.getenv('SALESFORCE_USERNAME')
            password = os.getenv('SALESFORCE_PASSWORD')
            security_token = os.getenv('SALESFORCE_TOKEN')
            domain = os.getenv('SALESFORCE_DOMAIN')

            sf = Salesforce(username=username, password=password, security_token=security_token, domain=domain)
            bulk = SalesforceBulk(sessionId=sf.session_id, host=sf.sf_instance)
            logger.info("Successfully authenticated to Salesforce.")
            cls.sf_rest_session = sf
            cls.sf_bulk_session = bulk
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            cls.sf_rest_session, cls.sf_bulk_session = None, None

    @classmethod
    def get_sessions(cls):
        """Возвращает текущие сессии для REST и Bulk API, инициирует новую сессию при необходимости."""
        # Проверяем, инициированы ли сессии, и если нет — инициализируем их один раз для всех экземпляров
        if not cls.sf_rest_session or not cls.sf_bulk_session:
            cls.initialize_salesforce_session()
        return cls.sf_rest_session, cls.sf_bulk_session


class SalesforceFileManager(SalesforceAuthentication):
    
    def __init__(self, content_document_id: str, save_folder: str):
        super().__init__()
        self.save_folder = save_folder
        self.content_document_id = content_document_id

    def download_and_save_file(self) -> Optional[str]:
        """Загружает последний файл ContentVersion из Salesforce и сохраняет его в указанной папке."""
        try:
            if not self.sf_rest_session:
                raise Exception('Salesforce REST session not initialized')

            query = f"""
            SELECT Id, Title, VersionData, FileExtension
            FROM ContentVersion
            WHERE ContentDocumentId = '{self.content_document_id}'
            ORDER BY LastModifiedDate DESC
            LIMIT 1
            """
            result = self.sf_rest_session.query(query)

            if not result['records']:
                raise Exception(f"No ContentVersion found for ContentDocumentId {self.content_document_id}")

            content_version = result['records'][0]
            content_version_id = content_version['Id']
            file_extension = content_version['FileExtension']
            file_title = content_version['Title']

            if file_extension != 'csv':
                raise Exception('File extension is incorrect')

            download_url = f"https://{self.sf_rest_session.sf_instance}/services/data/v61.0/sobjects/ContentVersion/{content_version_id}/VersionData"
            headers = {'Authorization': f'Bearer {self.sf_rest_session.session_id}'}
            response = requests.get(download_url, headers=headers)

            if response.status_code != 200:
                raise Exception(f"Error downloading file: {response.content}")

            if not os.path.exists(self.save_folder):
                os.makedirs(self.save_folder)

            self.file_path = os.path.join(self.save_folder, f"{file_title}.{file_extension}")
            with open(self.file_path, 'wb') as file:
                file.write(response.content)

            logger.info(f"File successfully downloaded and saved at {self.file_path}")
            return self.file_path
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error during file download: {str(e)}")
        except Exception as e:
            logger.error(f"Error occurred during file download: {str(e)}")
        return None


class BulkLoadProcessor(SalesforceAuthentication):
    def __init__(self):
        super().__init__()
        self.load_data = []

    def add_load(self, load_record):
        self.load_data.append(load_record)

    def send_bulk_data(self):
        df = pd.DataFrame(self.load_data)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.csv_data = csv_buffer.getvalue()

        if not self.csv_data.strip():
            logger.info("No data to send.")
            return

        try:
            job = self.sf_bulk_session.create_insert_job("Load__c", contentType='CSV')
            batch = self.sf_bulk_session.post_batch(job, self.csv_data)
            logger.debug(f"Batch response: {batch}")
            self.sf_bulk_session.wait_for_batch(job, batch)
            logger.info(f"Bulk operation completed for {len(self.load_data)} records.")
        except AttributeError as e:
            logger.error(f"Method not found: {e}")
        except Exception as e:
            logger.error(f"An error occurred during bulk processing: {e}")
        finally:
            self.sf_bulk_session.close_job(job)
            self.csv_data = ""


class ObjectMapper(SalesforceAuthentication):
    def __init__(self):
        super().__init__()

    def get_broker_map(self, customers):
        self.broker_map = {}
        for customer in customers:
            broker_name = ' '.join(customer.split()[:-1])
            broker = self.find_broker_by_name(broker_name)
            if not broker:
                broker = self.create_broker_in_account(broker_name)
            self.broker_map[broker_name] = broker
        return self.broker_map

    def find_broker_by_name(self, name):
        query = f"SELECT Id, Name FROM Account WHERE Name = '{name}'"
        result = self.sf_rest_session.query(query)
        return result['records'][0] if result['records'] else None

    def create_broker_in_account(self, name):
        account_data = {'Name': name, 'Type': 'Broker'}
        return self.sf_rest_session.Account.create(account_data)

    def get_driver_map(self, drivers):
        self.driver_map = {}
        for driver in drivers:
            driver_names = [name.strip() for name in driver.split(';')]
            for driver_name in driver_names:
                if not driver_name:
                    continue
                driver_account = self.find_driver_by_name(driver_name)
                if driver_account:
                    self.driver_map[driver_name] = {
                        'OWN_MC__c': driver_account.get('OWN_MC__c'),
                        'Id': driver_account.get('Id'),
                        'DRIVER_ID__c': driver_account.get('DRIVER_ID__c')
                    }
                else:
                    logger.warning(f"No driver account found for name: '{driver_name}'")
        return self.driver_map

    def find_driver_by_name(self, driver_name):
        try:
            escaped_driver_name = driver_name.replace("'", "\\'").upper()
            query = f"""
            SELECT Id, Name, OWN_MC__c, DRIVER_ID__c
            FROM Account
            WHERE RecordType.DeveloperName = 'DriverAccount' AND Name = '{escaped_driver_name}'
            """
            result = self.sf_rest_session.query(query)
            records = result['records']
            if not records:
                return None
            elif len(records) > 1:
                logger.warning(f"Multiple driver accounts found for name '{driver_name}'. Using the first one.")
            return records[0]
        except Exception as e:
                logger.error(f"Error querying driver by name '{driver_name}': {e}")
                return None


class Job(SalesforceFileManager, ObjectMapper, BulkLoadProcessor):
    def __init__(self, content_document_id: str, save_folder: str):
        # Инициализация всех родительских классов
        SalesforceFileManager.__init__(self, content_document_id, save_folder)
        ObjectMapper.__init__(self)
        BulkLoadProcessor.__init__(self)

        # Загрузка файла
        self.file_path = self.download_and_save_file()
        if not self.file_path:
            logger.error("Failed to download file.")
            raise Exception("File download failed.")

    def process_load_records(self, df):
        """Обрабатывает строки DataFrame и добавляет их в bulk загрузку."""
        for index, row in df.iterrows():
            try:
                load_data = {
                    'Name': row['Trip ID'].split()[-1],  
                    'Load_Number__c': row['Trip ID'].split()[-1],
                    'LINEHAUL_RATE__c': float(row['Estimated Cost'].replace('$', '').strip()) if isinstance(row['Estimated Cost'], str) else row['Estimated Cost'],
                    'EQUIPMENT_TYPE__c': 'DRY VAN',
                    'NOTES__c': row['Driver Name'].split(';') if ';' in row['Driver Name'] else row['Driver Name'],
                    'STATUS__c': row['Load Execution Status'],
                }
                self.add_load(load_data)
            except Exception as e:
                logger.error(f'Error processing load record at index {index}: {e}')

        # Отправляем bulk данные
        self.send_bulk_data()

    def process_file(self):
        """Чтение и обработка загруженного файла CSV."""
        if not os.path.exists(self.file_path):
            logger.error(f"File not found: {self.file_path}")
            return

        try:
            df = pd.read_csv(self.file_path)
            data = df.loc[:, ["Trip ID", "Driver Name", "Estimated Cost", "Load Execution Status"]]
            drivers = data['Driver Name'].tolist()

            # Создаем карту водителей
            driver_map = self.get_driver_map(drivers)

            # Обработка загруженных данных
            self.process_load_records(data)

        except Exception as e:
            logger.error(f"Error processing file {self.file_path}: {e}")
