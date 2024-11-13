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
            self.sf_bulk_session.close_job(job)
            self.csv_data = ""



class LoadRecord(BulkLoadProcessor, SalesforceAuthentication):

    

    def __init__(self, file_path: str):
        # Инициализация всех родительских классов
        SalesforceAuthentication.__init__(self)
        BulkLoadProcessor.__init__(self)
        self.file_path = file_path

    def process_load_records(self, df):
        """Обрабатывает строки DataFrame и добавляет их в bulk загрузку."""
        for index, row in df.iterrows():
            try:
                load_data = {
                                'Name': row['load'],
                                'Load_Number__c': row['load'],
                                'LINEHAUL_RATE__c': float(row['linehaul_total']),
                                'EQUIPMENT_TYPE__c': 'DRY VAN',
                                'NOTES__c': row['driver'],
                                'STATUS__c': row['status'],
                                'IsHistory__c': 'true'
                            }
                self.add_load(load_data)
            except Exception as e:
                logger.error(f'Error processing load record at index {index}: {e}')

        # Отправляем bulk данные
        self.send_bulk_data('Load__c')
    
    

    def process_file(self):
        """Чтение и обработка загруженного файла CSV."""
        if not os.path.exists(self.file_path):
            logger.error(f"File not found: {self.file_path}")
            return

        try:
            self.df = pd.read_excel(self.file_path)
            self.df.columns = [
                'company_load_number',
                'contract_spot',
                'sales_rep',
                'customer',
                'position',
                'status',
                'number_of_picks',
                'pu_info',
                'pu_state_code',
                'pu_time',
                'driver_pu_time',
                'number_of_drops',
                'del_info',
                'del_state_code',
                'del_time',
                'driver_del_time',
                'driver',
                'linehaul',
                'fuel_surcharge',
                'lumper',
                'linehaul_total',
                'empty_miles',
                'loaded_miles',
                'dollar_per_mile_loaded',
                'dollar_per_mile_total',
                'actions'
            ]
            
            self.df = self.df.loc[2001:3000, ['customer',
                    'status',
                    'pu_info',
                    'pu_state_code',
                    'pu_time',
                    'del_info',
                    'del_state_code',
                    'del_time',
                    'driver',
                    'linehaul_total',
                    'lumper']]
            
            self.df['load'] = self.df.customer.map(lambda i: i.split(' ')[-1])
            self.df.customer = self.df.customer.apply(lambda i: " ".join(i.split(' ')[:-1]))
            self.df['pu_city'] = self.df.pu_info.apply(lambda i: i.split(', ')[0])
            self.df['del_city'] = self.df.del_info.apply(lambda i: i.split(', ')[0])
            self.df['driver_id'] = self.df.driver.apply(lambda i: i.split(' - ')[0] if pd.notna(i) else i)
            self.df.driver = self.df.driver.apply(lambda i: i.split(' - ')[1].replace(' (100.0%)', '') if pd.notna(i) else i)
            self.df.driver = self.df.driver.fillna('')
            self.df.driver_id = self.df.driver_id.fillna('')

            self.process_load_records(self.df)

        except Exception as e:
            logger.error(f"Error processing file {self.file_path}: {e}")


class PickupDelivery(BulkLoadProcessor, SalesforceAuthentication):
    
    def __init__(self, file_path: str):
        # Инициализация всех родительских классов
        SalesforceAuthentication.__init__(self)
        BulkLoadProcessor.__init__(self)
        self.file_path = file_path

        
    def date(self, pu_time: str) -> pd.DataFrame:
        # Extract date, start time, end time, and timezone using regex
        match = pd.Series([pu_time]).str.extract(r'(?P<date>(\d{2})/(\d{2})/(\d{4}))\s+(?P<start_time>\d+:\d+)\s*-\s*(?P<end_time>\d+:\d+)(?P<timezone>[A-Z]+)')

        # Rename columns to make extraction clearer
        match.columns = ['date', 'month', 'day', 'year', 'start_time', 'end_time', 'timezone']
        return match.iloc[0]  # Return the extracted row as a dictionary-like Series

    def appointment_date(self, pu_time: str) -> list:
        # Handle None or empty input
        if not pu_time:
            return None

        # Extract date details from the date function
        df = self.date(pu_time)

        # Format into desired date-time strings
        start_datetime = f"{df['year']}-{df['month']}-{df['day']}T{df['start_time']}:00"
        end_datetime = f"{df['year']}-{df['month']}-{df['day']}T{df['end_time']}:00"

        return [start_datetime, end_datetime]
    
    def picup_dlvr_loader(self, df):
        
        for index, row in df.iterrows():
            try:
                
                 # Create load data for stop 1 (Pickup)
                load_data_1 = {
                    'LOAD__r.Load_Number__c': row['load'],
                    'Name': row['pu_info'],
                    'TYPE__c': 'Pickup',
                    'APPOITMENT_START__c': self.appointment_date(row['pu_time'])[0],
                    'APPOITMENT_END__c': self.appointment_date(row['pu_time'])[1],
                    'LOCATION__City__s': row['pu_city'],
                    'LOCATION__CountryCode__s': 'US',
                    'LOCATION__PostalCode__s': 'zip',
                    'LOCATION__StateCode__s': row['pu_state_code'],
                    'LOCATION__Street__s': 'st'
                }
                
                self.add_load(load_data_1)

                load_data_2 = {
                    'LOAD__r.Load_Number__c': row['load'],
                    'Name': row['del_info'],
                    'TYPE__c': 'Delivery',
                    'APPOITMENT_START__c': self.appointment_date(row['pu_time'])[0],
                    'APPOITMENT_END__c': self.appointment_date(row['pu_time'])[0],
                    'LOCATION__City__s':row['del_city'],
                    'LOCATION__CountryCode__s':'US',
                    'LOCATION__PostalCode__s': 'zip',
                    'LOCATION__StateCode__s': row['del_state_code'],
                    'LOCATION__Street__s': 'st'
                }

                # Append both load data dictionaries to the result list
                self.add_load(load_data_2)

            except Exception as e:
                        logger.error(f'Error processing load record at index {index}: {e}')
        
        self.send_bulk_data('Stop_Position__c')

    def process_file(self):
        """Чтение и обработка загруженного файла CSV."""

        try:
            self.df = pd.read_excel(self.file_path)
            self.df.columns = [
                'company_load_number',
                'contract_spot',
                'sales_rep',
                'customer',
                'position',
                'status',
                'number_of_picks',
                'pu_info',
                'pu_state_code',
                'pu_time',
                'driver_pu_time',
                'number_of_drops',
                'del_info',
                'del_state_code',
                'del_time',
                'driver_del_time',
                'driver',
                'linehaul',
                'fuel_surcharge',
                'lumper',
                'linehaul_total',
                'empty_miles',
                'loaded_miles',
                'dollar_per_mile_loaded',
                'dollar_per_mile_total',
                'actions'
            ]
            
            self.df = self.df.loc[2001:3000, ['customer',
                    'status',
                    'pu_info',
                    'pu_state_code',
                    'pu_time',
                    'del_info',
                    'del_state_code',
                    'del_time',
                    'driver',
                    'linehaul_total',
                    'lumper']]
            
            self.df['load'] = self.df.customer.map(lambda i: i.split(' ')[-1])
            self.df.customer = self.df.customer.apply(lambda i: " ".join(i.split(' ')[:-1]))
            self.df['pu_city'] = self.df.pu_info.apply(lambda i: i.split(', ')[0])
            self.df['del_city'] = self.df.del_info.apply(lambda i: i.split(', ')[0])
            self.df['driver_id'] = self.df.driver.apply(lambda i: i.split(' - ')[0] if pd.notna(i) else i)
            self.df.driver = self.df.driver.apply(lambda i: i.split(' - ')[1].replace(' (100.0%)', '') if pd.notna(i) else i)
            self.df.driver = self.df.driver.fillna('')
            self.df.driver_id = self.df.driver_id.fillna('')

            self.picup_dlvr_loader(self.df)

        except Exception as e:
            logger.error(f"Error processing file {self.file_path}: {e}")
        

    

        
        

    