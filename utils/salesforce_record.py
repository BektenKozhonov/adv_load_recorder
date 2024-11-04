import logging
from bulk_load_processor import BulkLoadProcessor

logger = logging.getLogger(__name__)


def process_load_records(df, broker_map, driver_map, sf_bulk_session):
    bulk_processor = BulkLoadProcessor()

    for index, row in df.iterrows():
        try:
            load_data = {
                'Name': row['Customer'].split()[-1],  # Извлекаем номер груза
                'Load_Number__c': row['Customer'].split()[-1],
                'MC__c': driver_map.get(row['Driver'].split(' ')[0].strip(), {}).get('OWN_MC__c', 'Default MC'),
                'LINEHAUL_RATE__c': float(row['Linehaul Total'].replace('$', '').strip()) if row[
                    'Linehaul Total'] else 0.0,
                'NOTES__c': row.get('Company Load#', 'No notes'),
                'EQUIPMENT_TYPE__c': 'DRY VAN',
                'STATUS__c': row['Status'],
                'BROKER__c': broker_map.get(' '.join(row['Customer'].split()[:-1]), 'Default Broker ID'),
            }

            bulk_processor.add_load(load_data)
        except Exception as e:
            logger.error(f'Error processing load record: {e}')

    bulk_processor.send_bulk_data(sf_bulk_session)
