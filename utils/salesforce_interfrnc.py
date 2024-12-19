import logging
import pandas as pd
from io import StringIO
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from dotenv import load_dotenv
import os
import requests
from typing import Optional, List


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

            if not all([username, password, security_token, domain]):
                raise ValueError("One or more Salesforce authentication environment variables are missing.")


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

class BulkLoadProcessor(SalesforceAuthentication):
    def __init__(self):
        super().__init__()
        self.load_data = []

    def add_load(self, load_record):
        self.load_data.append(load_record)

    def send_bulk_data(self, text):
        df = pd.DataFrame(self.load_data)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.csv_data = csv_buffer.getvalue()

        if not self.csv_data.strip():
            logger.info("No data to send.")
            return
        
        job = None
        try:
            job = self.sf_bulk_session.create_insert_job(f"{text}", contentType='CSV')
            batch = self.sf_bulk_session.post_batch(job, self.csv_data)
            logger.debug(f"Batch response: {batch}")
            self.sf_bulk_session.wait_for_batch(job, batch)
            logger.info(f"Bulk operation completed for {len(self.load_data)} records.")
        except AttributeError as e:
            logger.error(f"Method not found: {e}")
        except Exception as e:
            logger.error(f"An error occurred during bulk processing: {e}")
        finally:
            if job:
                self.sf_bulk_session.close_job(job)
            self.load_data = []



class ObjectMapper(SalesforceAuthentication):
    def __init__(self):
        super().__init__()
        self.broker_map = {}
        # Настройка логирования
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def get_broker_map(self, broker_name: str) -> dict:
        self.logger.info(f"Fetching broker map for broker: {broker_name}")
        
        broker = self.find_broker_by_name(broker_name)
        if not broker:
            self.logger.warning(f"Broker '{broker_name}' not found, creating a new one.")
            broker = self.create_broker_in_account(broker_name)

        self.logger.info(f"Broker map fetched for broker: {broker_name}")
        return self.broker_map

    def find_broker_by_name(self, name):
        self.logger.info(f"Searching for broker with name: {name}")
        query = f"SELECT Id, Name FROM Account WHERE Name = '{name}'"
        
        try:
            result = self.sf_rest_session.query(query)
            if result['records']:
                self.logger.info(f"Broker '{name}' found with ID: {result['records'][0]['Id']}")
            else:
                self.logger.warning(f"No broker found with name: {name}")
            return result['records'][0] if result['records'] else None
        except Exception as e:
            self.logger.error(f"Error during broker search: {e}")
            raise

    def create_broker_in_account(self, name):
        self.logger.info(f"Creating a new broker account with name: {name}")
        try:
            if 'AMAZON' in name.upper():
                account_data = {'Name': name, 
                                'Type': 'Broker', 
                                'AMAZON__c': True}
                self.logger.debug(f"Account data for AMAZON: {account_data}")
            else:
                account_data = {'Name': name, 
                                'Type': 'Broker', 
                                'STREETLOAD__c': True, 
                                'PROOF_OF_DELIVERY__c': True, 
                                'RATE_CONFIRMATION__c': True}
                self.logger.debug(f"Account data for other broker: {account_data}")
                
            account = self.sf_rest_session.Account.create(account_data)
            self.logger.info(f"Broker '{name}' created with ID: {account['id']}")
            return account
        except Exception as e:
            self.logger.error(f"Error during broker creation: {e}")
            raise


class TripSetter(SalesforceAuthentication):
    def __init__(self, save_folder: str):
        super().__init__()
        self.save_folder = save_folder

    def get_data_csv(self, result) -> Optional[pd.DataFrame]:
        """Сохраняет данные в CSV, если записи существуют."""
        try:
            if not result or 'records' not in result or not result['records']:
                logger.warning("No records found for the provided query")
                return None

            # Сохранение данных в CSV
            df = pd.DataFrame(result['records']).drop(columns='attributes', errors='ignore')
            return df
        except Exception as e:
            logger.error(f"Error saving data to CSV: {str(e)}")
            return None

    def execute_batched_query(self, query_template: str, load_numbers: list, batch_size: int, file_suffix: str) -> str:
        """Executes batched queries and saves the results in a CSV file."""
        df = pd.DataFrame()  # Initialize an empty DataFrame

        for i in range(0, len(load_numbers), batch_size):
            batch = load_numbers[i:i + batch_size]
            load_numbers_str = ','.join([f"'{num}'" for num in batch])
            query = query_template.format(load_numbers_str=load_numbers_str)

            try:
                result = self.sf_rest_session.query(query)
                middle = self.get_data_csv(result)
                if middle is not None:
                    df = pd.concat([df, middle], ignore_index=True)  # Safely append results
                else:
                    logger.warning(f"No data for batch {i} to {i + batch_size - 1}")
            except Exception as query_error:
                logger.error(f"Query failed for batch {i} to {i + batch_size - 1}: {str(query_error)}")
        
        # Construct file path with suffix
        file_name = f'{file_suffix}.csv'
        file_path = os.path.join(self.save_folder, file_name)
        df.to_csv(file_path, index=False)
        logger.info(f"Data successfully saved to {file_path}")

        return file_path

    def making_trip_sql_request(self, load_numbers: List[str]) -> Optional[str]:
        """Скачивает CSV с информацией о поездках."""
        try:
            os.makedirs(self.save_folder, exist_ok=True)
            if not self.sf_rest_session:
                raise Exception('Salesforce REST session not initialized')

            query_template = """
                SELECT Id, Load_Number__c, (SELECT Id, TYPE__c FROM Stop_Positions__r) 
                FROM Load__c 
                WHERE Load_Number__c IN ({load_numbers_str})
            """
            return self.execute_batched_query(query_template, load_numbers, batch_size=200, file_suffix='stop_pos_id')

        except Exception as e:
            logger.error(f"Error occurred during trip SQL request: {str(e)}")
            return None

    def making_driver_sql_request(self, load_numbers: List[str]) -> Optional[str]:
        """Скачивает CSV с информацией о водителях."""
        try:
            os.makedirs(self.save_folder, exist_ok=True)
            if not self.sf_rest_session:
                raise Exception('Salesforce REST session not initialized')

            query_template = """
                SELECT Id, DRIVER_ID__c, FirstName, LastName, 
                (SELECT Id, TYPE__c, END_DATE__c, UNIT__c FROM Vehicle_History__r WHERE END_DATE__c = null) 
                FROM Account 
                WHERE RecordType.DeveloperName = 'DriverAccount'
                AND DRIVER_ID__c IN ({load_numbers_str})
            """
            return self.execute_batched_query(query_template, load_numbers, batch_size=200, file_suffix='driver_id')

        except Exception as e:
            logger.error(f"Error occurred during driver SQL request: {str(e)}")
            return None
    

    