import logging
import pandas as pd
from utils.bulk_load_processor import BulkLoadProcessor
from utils.load_broker_driver_maps import get_broker_map, get_driver_map

logger = logging.getLogger(__name__)


def process_load_records(df, broker_map, driver_map, sf_bulk_session):
    bulk_processor = BulkLoadProcessor()

    for index, row in df.iterrows():
        try:
            load_data = {
                'Name': row['Driver Name'].split()[-1],  # Извлекаем номер груза
                'Load_Number__c': row['Customer'].split()[-1],
                'MC__c': driver_map.get(row['Trip ID'].split(' ')[0].strip(), {}).get('OWN_MC__c', 'Default MC'),
                'LINEHAUL_RATE__c': float(row['Estimated Cost'].replace('$', '').strip()) if row[
                    'Linehaul Total'] else 0.0,
                'EQUIPMENT_TYPE__c': 'DRY VAN',
                'STATUS__c': row['Load Execution Status'],
            }

            bulk_processor.add_load(load_data)
        except Exception as e:
            logger.error(f'Error processing load record: {e}')

    bulk_processor.send_bulk_data(sf_bulk_session)

def process_file(file_path, sf_rest_session, sf_bulk_session):
    """Чтение и обработка загруженного файла .xlsx."""
    df = pd.read_excel(file_path)
    data = df.loc[:, ["Trip ID", "Driver Name", "Estimated Cost", "Load Execution Status"]]
    customers = data['Customer'].tolist()
    drivers = data['Driver'].tolist()

    broker_map = get_broker_map(customers, sf_rest_session)
    driver_map = get_driver_map(drivers, sf_rest_session)

    # Передача DataFrame, карты брокеров, карты водителей и сессии Bulk API в функцию обработки записей
    return process_load_records(df, broker_map, driver_map, sf_bulk_session)
