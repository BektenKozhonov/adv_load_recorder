import logging
import pandas as pd
import numpy as np
import os
from utils.salesforce_interfrnc import SalesforceAuthentication, BulkLoadProcessor, TripSetter   #, ObjectMapper
import re
from collections import OrderedDict
import ast

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataSet:
    def __init__(self, filepath_kgline: str, filepath_tutash: str):
        self.df = None
        self.dfkg = pd.read_excel(filepath_kgline)
        self.dftutash = pd.read_excel(filepath_tutash)
        self.process_df()
    
    def set_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df.rename(columns={
                    "Company Load#": "company_load_number",
                    "Contract/Spot": "contract_or_spot",
                    "Fleet manager": "fleet_manager",
                    "Sales Rep": "sales_rep",
                    "Customer": "customer",
                    "Position": "position",
                    "Status": "status",
                    "# of Picks": "number_of_picks",
                    "PU Info": "pu_info",
                    "PU State Code": "pu_state_code",
                    "PU Time": "pu_time",
                    "Driver PU Time": "driver_pickup_time",
                    "# of Drops": "number_of_drops",
                    "DEL Info": "del_info",
                    "DEL State Code": "del_state_code",
                    "DEL Time": "del_time",
                    "Driver DEL Time": "driver_delivery_time",
                    "Driver": "driver",
                    "Linehaul": "linehaul",
                    "Fuel Surcharge": "fuel_surcharge",
                    "Linehaul Total": "linehaul_total",
                    "Empty Miles": "empty_miles",
                    "Loaded Miles": "loaded_miles",
                    "$ per mile (loaded)": "dollar_per_mile_loaded",
                    "$ per mile (total)": "dollar_per_mile_total",
                    "Actions": "actions",
                    "Lumper": "lumper"
                }, inplace=True, errors='ignore')
        
        required_columns = [
            'customer', 'status', 'pu_info', 'pu_state_code', 'pu_time',
            'del_info', 'del_state_code', 'del_time', 'driver',
            'linehaul_total', 'lumper', 'empty_miles', 'loaded_miles'
        ]
        df = df[[col for col in required_columns if col in df.columns]]
        
        df['load'] = df['customer'].apply(lambda i: i.split(' ')[-1] if pd.notna(i) else '')
        df['customer'] = df['customer'].apply(lambda i: " ".join(i.split(' ')[:-1]) if pd.notna(i) else '')
        df['pu_city'] = df['pu_info'].apply(lambda i: i.split(', ')[0] if pd.notna(i) else '')
        df['del_city'] = df['del_info'].apply(lambda i: i.split(', ')[0] if pd.notna(i) else '')
        df['driver_id'] = df['driver'].apply(lambda i: i.split(' - ')[0] if pd.notna(i) else '')
        df['driver'] = df['driver'].apply(lambda i: i.split(' - ')[1].replace(' (100.0%)', '') if pd.notna(i) and ' - ' in i else '')
        
        return df
    
    def process_df(self):
        self.dfkg = self.set_df(self.dfkg)
        self.dftutash = self.set_df(self.dftutash)
        self.df = pd.concat([self.dftutash, self.dfkg], ignore_index=True)
        self.df = self.df[~self.df['load'].duplicated()]


    


class TripDataset(DataSet, TripSetter):
    def __init__(self, filepath_kgline: str, filepath_tutash: str, savepath: str):
        DataSet.__init__(self, filepath_kgline, filepath_tutash)
        TripSetter.__init__(self, savepath)
        
        # Load file paths for trip and driver data
        del_pick_path = self.making_trip_sql_request(self.df['load'])
        trip_key_path = self.making_driver_sql_request(self.df['driver_id'])
        
        # Load CSV files
        self.csv_data = pd.read_csv(del_pick_path)
        self.trip_data = pd.read_csv(trip_key_path)
        
        # Process data
        self.process_csv_data()
        self.process_trip_data()
        self.data_merge()

    def extract_pickup_and_delivery_ids(self, data):
        """
        Extracts Pickup and Delivery IDs from the given data in dictionary/OrderedDict format.
        """
        try:
            if isinstance(data, str):
                parsed_dict = ast.literal_eval(data.replace("OrderedDict", ""))
            elif isinstance(data, (dict, OrderedDict)):
                parsed_dict = data
            else:
                raise ValueError("Invalid data format for Pickup/Delivery extraction.")
            
            records = parsed_dict.get('records', [])
            pickup_ids = [record['Id'] for record in records if record.get('TYPE__c') == 'Pickup']
            delivery_ids = [record['Id'] for record in records if record.get('TYPE__c') == 'Delivery']

            return {"pickup_ids": pickup_ids, "delivery_ids": delivery_ids}
        except Exception as e:
            logger.exception(f"Error extracting pickup and delivery IDs: {e}")
            return {"pickup_ids": [], "delivery_ids": []}

    def extract_vehicle_data(self, data):
        """
        Extracts 'Id', 'TYPE__c', 'UNIT__c' from OrderedDict data for vehicles.
        """
        try:
            vehicle_id = re.search(r"'Id': '(\w+)'", data)
            vehicle_type = re.search(r"'TYPE__c': '(\w+)'", data)
            unit_id = re.search(r"'UNIT__c': '(\w+)'", data)

            return {
                'vehicle_id': vehicle_id.group(1) if vehicle_id else None,
                'vehicle_type': vehicle_type.group(1) if vehicle_type else None,
                'unit_id': unit_id.group(1) if unit_id else None,
            }
        except Exception as e:
            logger.exception(f"Error extracting vehicle data: {e}")
            return {'vehicle_id': None, 'vehicle_type': None, 'unit_id': None}

    def process_csv_data(self):
        """
        Processes CSV data to extract pickup and delivery IDs and filter necessary columns.
        """
        try:
            self.csv_data['Stop_Positions__r'] = self.csv_data['Stop_Positions__r'].map(
                lambda i: self.extract_pickup_and_delivery_ids(i)
            )
            self.csv_data['pickup_id'] = self.csv_data['Stop_Positions__r'].apply(
                lambda x: x['pickup_ids'][0] if x['pickup_ids'] else None
            )
            self.csv_data['delivery_id'] = self.csv_data['Stop_Positions__r'].apply(
                lambda x: x['delivery_ids'][0] if x['delivery_ids'] else None
            )
            self.csv_data = self.csv_data[['Load_Number__c', 'pickup_id', 'delivery_id']]
            self.csv_data.columns = ['load', 'pickup_id', 'delivery_id']
        except Exception as e:
            logger.exception(f"Error processing CSV data: {e}")

    def process_trip_data(self):
        """
        Processes trip data to extract vehicle and driver information.
        """
        try:
            self.trip_data['Vehicle_History__r'] = self.trip_data['Vehicle_History__r'].map(
                lambda i: self.extract_vehicle_data(i) if pd.notna(i) else None
            )
            self.trip_data['vehicle_type'] = self.trip_data['Vehicle_History__r'].map(
                lambda x: x['vehicle_type'] if isinstance(x, dict) else None
            )
            self.trip_data['unit_id'] = self.trip_data['Vehicle_History__r'].map(
                lambda x: x['unit_id'] if isinstance(x, dict) and x.get('vehicle_type') == 'TRAILER' else None
            )
            self.trip_data['vehicle_id'] = self.trip_data['Vehicle_History__r'].map(
                lambda x: x['vehicle_id'] if isinstance(x, dict) and x.get('vehicle_type') == 'TRUCK' else None
            )
            self.trip_data = self.trip_data[['DRIVER_ID__c', 'vehicle_type', 'unit_id', 'vehicle_id']]
            self.trip_data.columns = ['driver_id', 'vehicle_type', 'unit_id', 'vehicle_id']
        except Exception as e:
            logger.exception(f"Error processing trip data: {e}")

    def data_merge(self):
        """
        Merges the main dataset with processed CSV and trip data.
        """
        try:
            # Merge CSV data
            self.df = pd.merge(self.df, self.csv_data, on='load', how='inner')

            # Ensure consistent data types for merging
            self.df['driver_id'] = self.df['driver_id'].astype(str)
            self.trip_data['driver_id'] = self.trip_data['driver_id'].astype(str)

            # Merge trip data
            self.df = pd.merge(self.df, self.trip_data, on='driver_id', how='inner')
        except Exception as e:
            logger.exception(f"Error merging data: {e}")

        



class LoadRecord(DataSet, BulkLoadProcessor, SalesforceAuthentication):
    
    def __init__(self, filepath_kgline: str, filepath_tutash: str):
        # Инициализация всех родительских классов
        DataSet.__init__(self, filepath_kgline, filepath_tutash)
        BulkLoadProcessor.__init__(self)
        SalesforceAuthentication.__init__(self)


    def process_load_records(self):
        #mapper = ObjectMapper()
        """Обрабатывает строки DataFrame и добавляет их в bulk загрузку."""
        for index, row in self.df.iterrows():
            try:
                load_data = {
                                'Name': row['load'],
                                'Load_Number__c': row['load'],
                                #'Broker__c':'',
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
    
    def __init__(self, filepath_kgline: str, filepath_tutash: str):
        # Инициализация всех родительских классов
        DataSet.__init__(self, filepath_kgline, filepath_tutash)
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

class Trip(TripDataset, BulkLoadProcessor):
    def __init__(self, filepath_kgline: str, filepath_tutash: str, save_folder: str):
        TripDataset.__init__(self, filepath_kgline, filepath_tutash, save_folder)

    def process_trip_records(self):
        """Обрабатывает строки DataFrame и добавляет их в bulk загрузку."""
        for index, row in self.df.iterrows():
            try:
                load_data = {
                                'AccountId__r.DRIVER_ID__c': row['driver_id'],
                                'LOAD__r.LOAD_NUMBER__c': row['load'],
                                'DEL__c': row['delivery_id'],
                                'DRIVER_PAY__c': float(row['linehaul_total']),
                                'DV__c': row['vehicle_id'], # поменяем sql из данных будем брат
                                'EMPTY_MI__c': row['empty_miles'],
                                'LOADED_MI__c': row['loaded_miles'],
                                'PICK__c': row['pickup_id'],
                                'PICKUP__c': row['pu_info'], 
                                'DELIVERY__c': row['del_info'],
                                'TRAILER__c': row['unit_id'], # поменяем sql из данных будем брать
                                'TRIP_STATUS__c': row['status']
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