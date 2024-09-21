from bulk_load_processor import BulkLoadProcessor

def process_load_records(df, broker_map, driver_map, sf_bulk_session):
    """
    Обрабатывает записи из DataFrame и отправляет их в Salesforce через Bulk API.
    
    :param df: DataFrame с данными
    :param broker_map: Карта брокеров
    :param driver_map: Карта водителей
    :param sf_bulk_session: Сессия Salesforce Bulk API
    """
    bulk_processor = BulkLoadProcessor()

    # Пример обработки файла для записи Load__c
    for index, row in df.iterrows():
        load_data = {
            'Name': row['Customer'].split()[-1],  # Извлекаем номер груза
            'Load_Number__c': row['Customer'].split()[-1],
            'MC__c': driver_map.get(row['Driver'].split(' ')[0].strip(), {}).get('OWN_MC__c'),
            'LINEHAUL_RATE__c': float(row['Linehaul Total'].replace('$', '').strip()),
            'NOTES__c': row['Company Load#'],
            'EQUIPMENT_TYPE__c': 'DRY VAN',
            'STATUS__c': row['Status'],
            'BROKER__c': broker_map.get(' '.join(row['Customer'].split()[:-1])),
        }

        bulk_processor.add_load(load_data)

    # Отправляем данные через Bulk API
    bulk_processor.send_bulk_data(sf_bulk_session)
