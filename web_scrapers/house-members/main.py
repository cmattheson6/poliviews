import scrapy
from scrapy.crawler import CrawlerProcess
from house_members.spiders.house_pols import HousePolsSpider
from house_members.settings import house_members_settings

import logging
from google.cloud import storage
import google.cloud.logging as logger
from house_members.pipelines import tmp_path
from datetime import date

gcs_creds = 'C:/Users/cmatt/Downloads/gce_creds.json'
project_id = 'politics-data-tracker-1'
bucket_name = 'poliviews'
pipeline_name = 'house_members'
blob_name = 'csvs/{0}/{0}_{1}.csv'.format(pipeline_name, date.today())
gcs_path = 'gs://' + bucket_name + '/' + blob_name

def main(data, context):
    try:
        storage_client = storage.Client()  # for cloud-based production
        stackdriver = logger.Client()
        stackdriver.setup_logging()
        logging.info('Accessed Stackdriver logging.')
    except:
        logging.info('Unable to passively access Google Cloud Storage. Attempting to access credentials ...')
        storage_client = storage.Client.from_service_account_json(gcs_creds)

    process = CrawlerProcess(settings=house_members_settings)
    logging.info('Initiated CrawlerProcess.')
    process.crawl(HousePolsSpider)
    logging.info('Start HousePolsSpider crawl.')
    process.start()

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(tmp_path)
    logging.info('File {0} uploaded to {1}'.format(
        tmp_path,
        gcs_path))
    pass

if __name__ == '__main__':
    main(data=None, context=None)

