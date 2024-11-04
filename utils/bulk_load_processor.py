import logging

logger = logging.getLogger(__name__)


class BulkLoadProcessor:
    def __init__(self):
        self.load_data = []

    def add_load(self, load_record):
        self.load_data.append(load_record)

    def send_bulk_data(self, sf_bulk_session):
        if not self.load_data:
            logger.info("No data to send.")
            return

        job = sf_bulk_session.create_insert_job("Load__c", contentType='JSON')
        batch = sf_bulk_session.post_bulk_batch(job, self.load_data)
        sf_bulk_session.wait_for_batch(job, batch)
        sf_bulk_session.close_job(job)
        logger.info(f"Bulk operation completed for {len(self.load_data)} records.")
        self.load_data.clear()
