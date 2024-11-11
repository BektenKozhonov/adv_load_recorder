import os
import gdown


def gdn(file_id: str, file_name: str = 'file_1', doc_type: str = 'csv', quiet: bool = False, folder: str = 'set/') -> None:
    # Убедитесь, что папка для загрузки существует
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    # Полный путь к файлу
    file_path = os.path.join(folder, f'{file_name}.{doc_type}')
    file = f'https://drive.google.com/uc?id={file_id}'
    
    # Загрузка файла
    gdown.download(file, file_path, quiet=quiet)

UPLOAD_FOLDER = 'set/'

# Пример использования функции для загрузки файла в папку 'set/'
gdn('1tyNgNcsUciQs7h-GJ5hMUsRTuLNlqcki', 'amazonfullfilment', folder=UPLOAD_FOLDER)
