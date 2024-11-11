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
        self.file_path = None
    
    def download_and_save_file(self) -> Optional[str]:
        """Загружает последний файл ContentVersion из Salesforce и сохраняет его в указанной папке."""
        try:
            # Генерируем путь к файлу заранее, чтобы проверить его наличие
            if not os.path.exists(self.save_folder):
                os.makedirs(self.save_folder)
                
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
            if os.path.isfile(self.file_path):
                logger.info(f"File already exists at {self.file_path}")
                return self.file_path  # Файл уже существует, возвращаем путь
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


# class ObjectMapper(SalesforceAuthentication):
#     def __init__(self):
#         super().__init__()

#     def get_broker_map(self, customers):
#         self.broker_map = {}
#         for customer in customers:
#             broker_name = ' '.join(customer.split()[:-1])
#             broker = self.find_broker_by_name(broker_name)
#             if not broker:
#                 broker = self.create_broker_in_account(broker_name)
#             self.broker_map[broker_name] = broker
#         return self.broker_map

#     def find_broker_by_name(self, name):
#         query = f"SELECT Id, Name FROM Account WHERE Name = '{name}'"
#         result = self.sf_rest_session.query(query)
#         return result['records'][0] if result['records'] else None

#     def create_broker_in_account(self, name):
#         account_data = {'Name': name, 'Type': 'Broker'}
#         return self.sf_rest_session.Account.create(account_data)

#     def get_driver_map(self, drivers):
#         self.driver_map = {}
#         for driver in drivers:
#             driver_names = [name.strip() for name in driver.split(';')]
#             for driver_name in driver_names:
#                 if not driver_name:
#                     continue
#                 driver_account = self.find_driver_by_name(driver_name)
#                 if driver_account:
#                     self.driver_map[driver_name] = {
#                         'OWN_MC__c': driver_account.get('OWN_MC__c'),
#                         'Id': driver_account.get('Id'),
#                         'DRIVER_ID__c': driver_account.get('DRIVER_ID__c')
#                     }
#                 else:
#                     logger.warning(f"No driver account found for name: '{driver_name}'")
#         return self.driver_map

#     def find_driver_by_name(self, driver_name):
#         try:
#             escaped_driver_name = driver_name.replace("'", "\\'").upper()
#             query = f"""
#             SELECT Id, Name, OWN_MC__c, DRIVER_ID__c
#             FROM Account
#             WHERE RecordType.DeveloperName = 'DriverAccount' AND Name = '{escaped_driver_name}'
#             """
#             result = self.sf_rest_session.query(query)
#             records = result['records']
#             if not records:
#                 return None
#             elif len(records) > 1:
#                 logger.warning(f"Multiple driver accounts found for name '{driver_name}'. Using the first one.")
#             return records[0]
#         except Exception as e:
#                 logger.error(f"Error querying driver by name '{driver_name}': {e}")
#                 return None


class AmznFull:
    amazonfullfilment = pd.read_csv('./set/amazonfullfilment.csv')
    def __init__(self):       
        self.df = None  # Initialize df

    def filtering_data(self, amazon_code: str) -> pd.DataFrame:
        self.amazon_code = amazon_code
        self.df = self.amazonfullfilment[self.amazonfullfilment['CODE'] == self.amazon_code]
        if self.df.empty:
            self.df = pd.DataFrame([{
                    'CODE': self.amazon_code,
                    'Address': self.amazon_code,
                    'City': 'Not Found',
                    'State': 'IL',
                    'Country': 'USA',
                    'Zip Code': 'zip'
                }])
        return self.df

    def get_city(self) -> str:
        return self.df.iloc[0]['City'].capitalize() if not self.df.empty else ""

    def get_address(self) -> str:
        return self.df.iloc[0]['Address'] if not self.df.empty else ""

    def get_state(self) -> str:
        return self.df.iloc[0]['State'] if not self.df.empty else ""

    def get_zip(self) -> str:
        return self.df.iloc[0]['Zip Code'] if not self.df.empty else ""

    def get_country(self) -> str:
        country = self.df.iloc[0]['Country'].strip().upper()
        if country == 'USA':
            return 'US'
        else:
            raise Exception('Country is not USA')

    def get_country_(self) -> str:
        return self.df.iloc[0]['Country'].strip()

    def __call__(self) -> dict:
        self.filtering_data()  # Ensure filtering is done
        return {
            'city': self.get_city(),
            'address': self.get_address(),
            'state': self.get_state(),
            'zip': self.get_zip(),
            'country': self.get_country()
        }



class LoadRecord(SalesforceFileManager, BulkLoadProcessor):
    amazonfullfilment = pd.read_csv('./set/amazonfullfilment.csv')

    def __init__(self, content_document_id: str, save_folder: str):
        # Инициализация всех родительских классов
        SalesforceFileManager.__init__(self, content_document_id, save_folder)
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
        self.send_bulk_data('Load__c')

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
            #driver_map = self.get_driver_map(drivers)

            # Обработка загруженных данных
            self.process_load_records(data)

        except Exception as e:
            logger.error(f"Error processing file {self.file_path}: {e}")


class PickupDelivery(LoadRecord, AmznFull):
    
    def __init__(self, content_document_id: str, save_folder: str):
        # Инициализация всех родительских классов
        SalesforceFileManager.__init__(self, content_document_id, save_folder)
        BulkLoadProcessor.__init__(self)
        self.amazon_stop_1 = AmznFull()
        self.amazon_stop_2 = AmznFull()

        # Загрузка файла
        self.file_path = self.download_and_save_file()
        if not self.file_path:
            logger.error("Failed to download file.")
            raise Exception("File download failed.")
        
    def Name(self, city: str, state: str):
        return f'{city.capitalize()},{state}'

    def date_detection(self, date: str) -> dict:
        # Check if date is not None and is a string
        if date is None:
            raise ValueError("Date cannot be None")

        # Split date into parts
        date_parts = date.split('/')
        # Basic validation for expected length
        if len(date_parts) != 3:
            raise ValueError("Date format should be 'MM/DD/YYYY'")

        day, month, year = date_parts[1], date_parts[0], date_parts[2]
        return {'day': day, 'month': month, 'year': year}

    def appointment_date(self, date: str, UTC_offset: str, time: str) -> str:
        # Check if any input is None and handle it
        if not all([date, UTC_offset, time]):
            return None  # or provide a default, like an empty string ""

        # Ensure date format using date_detection
        date_parts = self.date_detection(date)
        return f"{date_parts['year']}-{date_parts['month']}-{date_parts['day']}T{time}:00.000{UTC_offset}00"
    
    def picup_dlvr_loader(self, df):
        
        for index, row in df.iterrows():
            try:
                
                # Ensure filtering is done for each stop
                self.amazon_stop_1.filtering_data(row['stop_1'])

                # Extract single values for date and time fields
                stop1_date_arrival = row.get('stop_1_planned_arrival_date')
                stop1_time_arrival = row.get('stop_1_planned_arrival_time')
                stop1_utc_offset = row.get('stop_1_utc_offset')
                stop1_date_dep = row.get('stop_1_planned_departure_date')
                stop1_time_dep = row.get('stop_1_planned_departure_time')


                # Create load data for stop 1 (Pickup)
                load_data_1 = {
                    'LOAD__r.Load_Number__c': row['trip_id'],
                    'Name': self.Name(city=self.amazon_stop_1.get_city(), state=self.amazon_stop_1.get_state()),
                    'TYPE__c': 'Pickup',
                    'APPOITMENT_START__c': self.appointment_date(stop1_date_arrival, stop1_utc_offset, stop1_time_arrival),
                    'APPOITMENT_END__c': self.appointment_date(stop1_date_dep, stop1_utc_offset, stop1_time_dep),
                    'LOCATION__City__s': self.amazon_stop_1.get_city(),
                    'LOCATION__CountryCode__s': self.amazon_stop_1.get_country(),
                    'LOCATION__PostalCode__s': self.amazon_stop_1.get_zip(),
                    'LOCATION__StateCode__s': self.amazon_stop_1.get_state(),
                    'LOCATION__Street__s': self.amazon_stop_1.get_address(),
                    'AWS_CODE__c': row['stop_1']
                }

                
                self.add_load(load_data_1)

                self.amazon_stop_2.filtering_data(row['stop_2'])

                stop2_date_arrival = row.get('stop_2_planned_arrival_date')
                stop2_time_arrival = row.get('stop_2_planned_arrival_time')
                stop2_date_dep = row.get('stop_2_planned_departure_date')
                stop2_time_dep = row.get('stop_2_planned_departure_time')
                stop2_utc_offset = row.get('stop_2_utc_offset')

                # Create load data for stop 2 (Delivery)
                load_data_2 = {
                    'LOAD__r.Load_Number__c': row['trip_id'],
                    'Name': self.Name(city=self.amazon_stop_2.get_city(), state= self.amazon_stop_2.get_state()),
                    'TYPE__c': 'Delivery',
                    'APPOITMENT_START__c': self.appointment_date(stop2_date_arrival, stop2_utc_offset, stop2_time_arrival),
                    'APPOITMENT_END__c': self.appointment_date(stop2_date_dep, stop2_utc_offset, stop2_time_dep),
                    'LOCATION__City__s': self.amazon_stop_2.get_city(),
                    'LOCATION__CountryCode__s': self.amazon_stop_2.get_country(),
                    'LOCATION__PostalCode__s': self.amazon_stop_2.get_zip(),
                    'LOCATION__StateCode__s': self.amazon_stop_2.get_state(),
                    'LOCATION__Street__s': self.amazon_stop_2.get_address(), 
                    'AWS_CODE__c': row['stop_2']
                }

                # Append both load data dictionaries to the result list
                self.add_load(load_data_2)

            except Exception as e:
                        logger.error(f'Error processing load record at index {index}: {e}')
        
        self.send_bulk_data('Stop_Position__c')

    def process_file(self):
        """Чтение и обработка загруженного файла CSV."""
        if not os.path.exists(self.file_path):
            logger.error(f"File not found: {self.file_path}")
            return

        try:
            df = pd.read_csv(self.file_path)
            df.columns = [
                            'block_id', 
                            'trip_id', 
                            'block_trip', 
                            'trip_stage', 
                            'load_id',
                            'facility_sequence', 
                            'load_execution_status', 
                            'transit_operator_type',
                            'driver_name', 
                            'equipment_type', 
                            'trailer_id', 
                            'tractor_vehicle_id',
                            'estimate_distance', 
                            'unit', 
                            'rate_type', 
                            'estimated_cost', 
                            'currency',
                            'truck_filter', 
                            'operator_id', 
                            'shipper_account', 
                            'sub_carrier',
                            'cr_id', 
                            'port_appointment_date', 
                            'port_appointment_time',
                            'port_pin_code', 
                            'spot_work', 
                            'contract_type', 
                            'contract_id', 
                            'stop_1',
                            'stop_1_utc_offset', 
                            'stop_1_planned_arrival_date',
                            'stop_1_planned_arrival_time', 
                            'stop_1_actual_arrival_date',
                            'stop_1_actual_arrival_time', 
                            'stop_1_planned_departure_date',
                            'stop_1_planned_departure_time', 
                            'stop_1_actual_departure_date',
                            'stop_1_actual_departure_time', 
                            'stop_1_container_id', 
                            'stop_2',
                            'stop_2_utc_offset', 
                            'stop_2_planned_arrival_date',
                            'stop_2_planned_arrival_time', 
                            'stop_2_actual_arrival_date',
                            'stop_2_actual_arrival_time', 
                            'stop_2_planned_departure_date',
                            'stop_2_planned_departure_time', 
                            'stop_2_actual_departure_date',
                            'stop_2_actual_departure_time', 
                            'stop_2_container_id'
            ]
            data = df.loc[:, [
                            'trip_id', 
                            'stop_1', 
                            'stop_1_planned_arrival_date', 
                            'stop_1_planned_arrival_time', 
                            'stop_1_planned_departure_date', 
                            'stop_1_planned_departure_time', 
                            'stop_2', 
                            'stop_2_planned_arrival_date', 
                            'stop_2_planned_arrival_time', 
                            'stop_2_planned_departure_date', 
                            'stop_2_planned_departure_time', 
                            'stop_1_utc_offset', 
                            'stop_2_utc_offset'
                        ]]


            # Обработка загруженных данных
            self.picup_dlvr_loader(data)

        except Exception as e:
            logger.error(f"Error processing file {self.file_path}: {e}")
        

    

        
        

    