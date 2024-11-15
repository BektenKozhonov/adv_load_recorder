import logging
import os
import glob
from utils.job import PickupDelivery, LoadRecord
from utils.salesforce_interfrnc import SalesforceAuthentication, TripSetter

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
            return

        # Process each file
        for file in excel_files:
            try:
                # Create Trip instance (inherits LoadRecord and PickupDelivery functionality)
                # load_instance = LoadRecord(file)
                pck_del_instance = PickupDelivery(file)

                # Process LoadRecord, PickupDelivery, and Trip data
                # load_instance.process_file()
                # pck_del_instance.process_file()
                
                # Download and save supportive files if required
                load_numbers = pck_del_instance.df['load'].tolist()
                trip_setter = TripSetter(SUPPORTIVE_FOLDER)
                trip_setter.download_and_save_file(load_numbers)

                logger.info(f"File {file} processed successfully")
            except Exception as e:
                logger.error(f"Error processing file {file}: {e}")
    except Exception as e:
        logger.error(f"Error in process_files: {e}")
        
if __name__ == '__main__':
    process_files()
