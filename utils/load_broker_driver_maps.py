import logging

logger = logging.getLogger(__name__)


def get_broker_map(customers, sf_rest_session):
    broker_map = {}
    for customer in customers:
        broker_name = ' '.join(customer.split()[:-1])
        broker = find_broker_by_name(broker_name, sf_rest_session)
        if not broker:
            broker = create_broker_in_account(broker_name, sf_rest_session)
        broker_map[broker_name] = broker
    return broker_map


def find_broker_by_name(name, sf_rest_session):
    query = f"SELECT Id, Name FROM Account WHERE Name = '{name}'"
    result = sf_rest_session.query(query)
    return result['records'][0] if result['records'] else None


def create_broker_in_account(name, sf_rest_session):
    account_data = {'Name': name, 'Type': 'Broker'}
    return sf_rest_session.Account.create(account_data)


def get_driver_map(drivers, sf_rest_session):
    driver_map = {}
    for driver in drivers:
        driver_id = driver.split(' ')[0].strip()
        driver_account = find_driver_by_id(driver_id, sf_rest_session)
        if driver_account:
            driver_map[driver_id] = {
                'OWN_MC__c': driver_account.get('OWN_MC__c'),
                'Id': driver_account.get('Id'),
                'DRIVER_ID__c': driver_account.get('DRIVER_ID__c')
            }
    return driver_map


def find_driver_by_id(driver_id, sf_rest_session):
    query = f"SELECT Id, DRIVER_ID__c, OWN_MC__c FROM Account WHERE RecordType.DeveloperName = 'DriverAccount' AND DRIVER_ID__c = '{driver_id}'"
    result = sf_rest_session.query(query)
    return result['records'][0] if result['records'] else None
