import logging
import os
import glob
from utils.load_data import SalesforceAuthentication, LoadRecord, PickupDelivery

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Путь для временного сохранения файлов
UPLOAD_FOLDER = 'temp/'

# Проверяем, существует ли папка, и создаем её при необходимости
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def process_files():
    try:
        # Инициализируем класс SalesforceAuthentication и получаем сессии
        auth = SalesforceAuthentication()
        sf_rest_session, sf_bulk_session = auth.get_sessions()
        
        # Проверка успешной аутентификации
        if not sf_rest_session or not sf_bulk_session:
            logger.error('Failed to initialize Salesforce session')
            return

        # Получаем список всех файлов .xlsx в папке
        excel_files = glob.glob(os.path.join(UPLOAD_FOLDER, "*.xlsx"))

        # Обработка каждого файла
        for file in excel_files:
            load_rec = LoadRecord(file)
            pick_dvr_rec = PickupDelivery(file)
            
            # Проверка, удалось ли загрузить файл
            if not all([load_rec.file_path, pick_dvr_rec.file_path]):
                logger.error('Failed to download and save file')
                return

            # Обработка загруженного файла
            load_rec.process_file()
            pick_dvr_rec.process_file()
            logger.info(f"File {file} processed successfully")
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")

if __name__ == '__main__':
    process_files()
