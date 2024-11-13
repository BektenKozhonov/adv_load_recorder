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