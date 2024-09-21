import logging

def get_broker_map(customers, sf_rest_session):
    broker_map = {}

    for customer in customers:
        # Извлечение имени брокера (до последнего пробела)
        broker_name = ' '.join(customer.split()[:-1])
        logging.info(f"Processing broker: {broker_name}")

        # Поиск брокера в Account
        broker = find_broker_by_name(broker_name, sf_rest_session)

        if not broker:
            # Если брокер не найден, создаем новый
            broker = create_broker_in_account(broker_name, sf_rest_session)
            logging.info(f"Created new broker: {broker_name}")
        else:
            logging.info(f"Found existing broker: {broker_name}")

        broker_map[broker_name] = broker
    
    return broker_map

def find_broker_by_name(name, sf_rest_session):
    logging.info(f"Searching for broker: {name}")
    query = f"SELECT Id, Name, Type, AMAZON__c FROM Account WHERE Name = '{name}'"
    result = sf_rest_session.query(query)
    
    if result['records']:
        logging.info(f"Broker {name} found in the system.")
        return result['records'][0]
    else:
        logging.warning(f"Broker {name} not found in the system.")
        return None

def create_broker_in_account(name, sf_rest_session):
    account_data = {
        'Name': name,
        'Type': 'Broker'
    }

    if 'Amazon' in name:
        account_data['AMAZON__c'] = True
        logging.info(f"Setting AMAZON__c to True for broker: {name}")
    else:
        account_data['RATE_CONFIRMATION__c'] = True
        account_data['PROOF_OF_DELIVERY__c'] = True
        account_data['STREETLOAD__c'] = True
        logging.info(f"Setting RATE_CONFIRMATION__c, PROOF_OF_DELIVERY__c, STREETLOAD__c to True for broker: {name}")
    
    broker = sf_rest_session.Account.create(account_data)
    logging.info(f"Broker {name} created in Salesforce with ID: {broker['id']}")

    return broker

def get_driver_map(drivers, sf_rest_session):
    driver_map = {}

    for driver in drivers:
        # Извлечение DRIVER_ID__c (всё до первого пробела)
        driver_id = driver.split(' ')[0].strip()
        logging.info(f"Processing driver: {driver}, extracted DRIVER_ID__c: {driver_id}")
        
        # Поиск водителя по DRIVER_ID__c и RecordType.DeveloperName = 'DriverAccount'
        driver_account = find_driver_by_id(driver_id, sf_rest_session)

        if driver_account:
            driver_map[driver_id] = {
                'OWN_MC__c': driver_account.get('OWN_MC__c'),
                'Id': driver_account.get('Id'),
                'DRIVER_ID__c': driver_account.get('DRIVER_ID__c')
            }
            logging.info(f"Driver found: {driver_map[driver_id]}")
        else:
            logging.warning(f"No account found for DRIVER_ID__c: {driver_id}")

    return driver_map

def find_driver_by_id(driver_id, sf_rest_session):
    query = f"""
    SELECT Id, DRIVER_ID__c, OWN_MC__c 
    FROM Account 
    WHERE RecordType.DeveloperName = 'DriverAccount' 
    AND DRIVER_ID__c = '{driver_id}'
    """
    logging.info(f"Executing query: {query}")
    result = sf_rest_session.query(query)

    if result['records']:
        logging.info(f"Found {len(result['records'])} records for DRIVER_ID__c: {driver_id}")
        return result['records'][0]
    else:
        logging.warning(f"No records found for DRIVER_ID__c: {driver_id}")
        return None
