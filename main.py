import logging
import os
import glob
from utils.job import PickupDelivery, LoadRecord, Trip
from utils.salesforce_interfrnc import SalesforceAuthentication

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Путь для временного сохранения файлов
UPLOAD_FOLDER = 'temp/'
SUPPORTIVE_FOLDER = 'set/'
# Проверяем, существует ли папка, и создаем её при необходимости
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def process_files():
    try:
        # Initialize Salesforce sessions
        auth = SalesforceAuthentication()
        sf_rest_session, sf_bulk_session = auth.get_sessions()
        
        # Check if sessions are valid
        if not sf_rest_session or not sf_bulk_session:
            logger.error('Failed to initialize Salesforce session')
            return

        # Get list of Excel files
        excel_files = glob.glob(os.path.join(UPLOAD_FOLDER, "*.xlsx"))
        if not excel_files:
            logger.info("No Excel files found for processing.")
            
        # Create Trip instance (inherits LoadRecord and PickupDelivery functionality)
        load_instance = LoadRecord(excel_files[0], excel_files[1])
        pck_del_instance = PickupDelivery(excel_files[0], excel_files[1])
        

        # Process LoadRecord, PickupDelivery, and Trip data
        # load_instance.process_file()
        # pck_del_instance.picup_dlvr_loader()
        
        trip_instance = Trip(excel_files[0], excel_files[1], SUPPORTIVE_FOLDER)
        trip_instance.process_trip_records()

        logger.info(f"File {excel_files} processed successfully")
        
    except Exception as e:
        logger.error(f"Error in process_files: {e}")
        
if __name__ == '__main__':
    process_files()
