import logging
import pandas as pd
import os
from utils.salesforce_interfrnc import SalesforceAuthentication, BulkLoadProcessor, TripSetter
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataSet:
    def __init__(self, filepath):
        self.df = pd.read_excel(filepath)
        self.process_df()
    
    def process_df(self):
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
        
        self.df = self.df.loc[2001:2100, [
            'customer',
            'status',
            'pu_info',
            'pu_state_code',
            'pu_time',
            'del_info',
            'del_state_code',
            'del_time',
            'driver',
            'linehaul_total',
            'lumper',
            'empty_miles',
            'loaded_miles'
        ]]
        
        self.df['load'] = self.df.customer.map(lambda i: i.split(' ')[-1])
        self.df['customer'] = self.df.customer.apply(lambda i: " ".join(i.split(' ')[:-1]))
        self.df['pu_city'] = self.df.pu_info.apply(lambda i: i.split(', ')[0])
        self.df['del_city'] = self.df.del_info.apply(lambda i: i.split(', ')[0])
        self.df['driver_id'] = self.df.driver.apply(lambda i: i.split(' - ')[0] if pd.notna(i) else '')
        self.df['driver'] = self.df.driver.apply(lambda i: i.split(' - ')[1].replace(' (100.0%)', '') if pd.notna(i) else '')
        self.df['driver'] = self.df.driver.fillna('')
        self.df['driver_id'] = self.df.driver_id.fillna('')




class LoadRecord(DataSet, BulkLoadProcessor, SalesforceAuthentication):
    
    def __init__(self, file_path: str):
        # Инициализация всех родительских классов
        DataSet.__init__(self, file_path)
        BulkLoadProcessor.__init__(self)
        SalesforceAuthentication.__init__(self)


    def process_load_records(self):
        """Обрабатывает строки DataFrame и добавляет их в bulk загрузку."""
        for index, row in self.df.iterrows():
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
        self.process_load_records()


class PickupDelivery(DataSet, BulkLoadProcessor, SalesforceAuthentication):
    
    def __init__(self, file_path: str):
        # Инициализация всех родительских классов
        DataSet.__init__(self, file_path)
        BulkLoadProcessor.__init__(self)
        SalesforceAuthentication.__init__(self)

        
    def parse_date(self, pu_time: str) -> dict:
        if not pu_time or not isinstance(pu_time, str):
            return None

        match = re.match(
            r'(?P<date>(?P<month>\d{2})/(?P<day>\d{2})/(?P<year>\d{4}))\s+'
            r'(?P<start_time>\d+:\d+)\s*-\s*(?P<end_time>\d+:\d+)(?P<timezone>[A-Z]+)',
            pu_time
        )
        if not match:
            logger.error(f"Time format is incorrect: {pu_time}")
            return None
        return match.groupdict()

    def appointment_date(self, pu_time: str) -> list:

        parsed = self.parse_date(pu_time)
        if not parsed:
            return [None, None]

        start_datetime = f"{parsed['year']}-{parsed['month']}-{parsed['day']}T{parsed['start_time']}:00"
        end_datetime = f"{parsed['year']}-{parsed['month']}-{parsed['day']}T{parsed['end_time']}:00"
        return [start_datetime, end_datetime]
    
    def picup_dlvr_loader(self):
        
        for index, row in self.df.iterrows():
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
                self.add_load(load_data_1)
                self.add_load(load_data_2)

            except Exception as e:
                        logger.error(f'Error processing load record at index {index}: {e}')
        
        self.send_bulk_data('Stop_Position__c')

    def process_file(self):
        """Чтение и обработка загруженного файла CSV."""
        self.picup_dlvr_loader()

class Trip(TripSetter, DataSet, BulkLoadProcessor, SalesforceAuthentication):
    def __init__(self, save_folder: str, file_folder: str):
        TripSetter.__init__(self, save_folder)
        PickupDelivery.__init__(self, file_folder)

    def process_trip_records(self):
        """Обрабатывает строки DataFrame и добавляет их в bulk загрузку."""
        for index, row in self.df.iterrows():
            try:
                load_data = {
                                'AccountId__c': row['driver_id'],
                                'LOAD__c': row['load'],
                                'DEL__c': '',
                                'DRIVER_PAY__c': float(row['linehaul_total']),
                                'EMPTY_MI__c': row['empty_miles'],
                                'LOADED_MI__c': row['loaded_miles'],
                                'PICK__c': row['pu_info'],
                                'PICKUP__c': '',
                                'PU_DATE__c': '',
                                'PU_DT__c': '',
                                'TRAILER__c': '', 
                                'TRIP_STATUS__c': '',
                                'TRUCK__c': ''
                            }
                
                self.add_load(load_data)
            except Exception as e:
                logger.error(f'Error processing load record at index {index}: {e}')

        # Отправляем bulk данные
        self.send_bulk_data('Trip__c')

    def process_file(self):
        """Чтение и обработка загруженного файла CSV."""

        try:
            self.process_trip_records()

        except Exception as e:
            logger.error(f"Error processing file {self.file_path}: {e}")