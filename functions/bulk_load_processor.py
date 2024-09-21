class BulkLoadProcessor:
    def __init__(self):
        self.load_data = []

    def add_load(self, load_record):
        """
        Добавляем подготовленную запись в список для последующей отправки.
        """
        self.load_data.append(load_record)

    def send_bulk_data(self, sf_bulk_session):
        """
        Отправляем накопленные данные через Bulk API.
        """
        if not self.load_data:
            print("No data to send.")
            return
        
        # Открываем работу для вставки записей Load__c
        job = sf_bulk_session.create_insert_job("Load__c", contentType='JSON')
        batch = sf_bulk_session.post_bulk_batch(job, self.load_data)
        sf_bulk_session.wait_for_batch(job, batch)
        sf_bulk_session.close_job(job)

        print(f"Bulk operation completed for {len(self.load_data)} records.")
        self.load_data.clear()  # Очищаем список после отправки
