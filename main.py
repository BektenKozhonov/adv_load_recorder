from flask import Flask, request, jsonify
from utils.load_data import SalesforceAuthentication, Job
import logging
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Путь для временного сохранения файлов
UPLOAD_FOLDER = 'temp/'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Проверяем, существует ли папка, и создаем её при необходимости
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)


@app.route('/receive_file', methods=['POST'])
def receive_file():
    content_document_id = request.json.get('ContentDocumentId')
    if not content_document_id:
        return jsonify({'error': 'ContentDocumentId is missing'}), 400

    try:
        # Инициализируем класс SalesforceAuthentication и получаем сессии
        auth = SalesforceAuthentication()
        sf_rest_session, sf_bulk_session = auth.get_sessions()
        
        # Проверка успешной аутентификации
        if not sf_rest_session or not sf_bulk_session:
            return jsonify({'error': 'Failed to initialize Salesforce session'}), 500

        # Создаем экземпляр Job для загрузки и обработки файла
        job = Job(content_document_id, app.config['UPLOAD_FOLDER'])
        
        # Проверка, удалось ли загрузить файл
        if not job.file_path:
            return jsonify({'error': 'Failed to download and save file'}), 500

        # Обработка загруженного файла
        job.process_file()
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        return jsonify({'error': 'Failed to process file'}), 500

    return jsonify({'message': 'File processed successfully'}), 200


if __name__ == '__main__':
    app.run(debug=True)