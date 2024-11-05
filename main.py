import os
from flask import Flask, request, jsonify
from utils.salesforce_auth import initialize_salesforce_session, download_and_save_file
from utils.salesforce_record import process_file
import pandas as pd

app = Flask(__name__)

# Путь для временного сохранения файлов
UPLOAD_FOLDER = 'temp/'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Глобальные переменные для хранения сессий Salesforce
sf_rest_session = None
sf_bulk_session = None


@app.route('/receive_file', methods=['POST'])
def receive_file():
    content_document_id = request.json.get('ContentDocumentId')
    if not content_document_id:
        return jsonify({'error': 'ContentDocumentId is missing'}), 400

    # Инициализируем Salesforce сессии, если они еще не инициализированы
    global sf_rest_session, sf_bulk_session
    if not sf_rest_session or not sf_bulk_session:
        sf_rest_session, sf_bulk_session = initialize_salesforce_session()
        print(sf_bulk_session, sf_rest_session)
    if not sf_rest_session or not sf_bulk_session:
        return jsonify({'error': 'Failed to initialize Salesforce session'}), 500

    # Загружаем файл из Salesforce и сохраняем его в папке
    file_path = download_and_save_file(content_document_id, app.config['UPLOAD_FOLDER'])
    if not file_path:
        return jsonify({'error': 'Failed to download and save file'}), 500

    # Обработка загруженного файла
    process_file(file_path)

    return jsonify({'message': 'File processed successfully'}), 200




if __name__ == '__main__':
    app.run(debug=True)
