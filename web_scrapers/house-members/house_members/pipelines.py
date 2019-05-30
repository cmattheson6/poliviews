"""
This pipeline will upload all items from the spiders to the proper Pub/Sub topic.
The rest of the processing will take place in Dataflow.
"""


import logging
import pandas as pd
from datetime import date
import os
import sys
import unidecode
from subprocess import Popen
import re
from google.cloud import storage

gcs_creds = 'C:/Users/cammatth/Downloads/gce_creds.json'
project_id = 'politics-data-tracker-1'
bucket_name = 'poliviews'
pipeline_name = 'house_members'
gcs_dirname = 'gs://{0}/{1}/csvs/{2}'.format(project_id, bucket_name, pipeline_name)
gcs_path = gcs_dirname + '/{0}_{1}.csv'.format(pipeline_name, date.today())
blob_name = 'csvs/{0}/{0}_{1}.csv'.format(pipeline_name, date.today())
tmp_dirname = '~/tmp'
tmp_path = tmp_dirname + '/{0}_{1}.csv'.format(pipeline_name, date.today())
rm_old_files = 'rm {0}/*'.format(gcs_dirname)

logging.basicConfig(level=logging.INFO)

try:
    cmd = Popen(rm_old_files, shell=True).stdout.read()
    print(cmd)
except Exception as e:
    print(e)

class PoliticiansPipeline(object):
    # set csv location and open it
    f= open(tmp_path, mode='a+')
    # storage_client = storage.Client()  # for cloud-based production
    storage_client = storage.Client.from_service_account_json(gcs_creds)  # only for local testing
    lst = []

    def process_item(self, item, spider):
        """We need to establish a an authorized connection to Google Cloud in order to upload to Google Pub/Sub.
        In order to host the spiders on Github, the service account credentials are housed on the Scrapy platform
        and dynamically created in the script."""

        # Add the item as a row in the csv here
        item = {k:unidecode.unidecode(v) for (k, v) in item.items()}
        item = {k: re.sub('\,', '', v) for (k, v) in item.items()}
        self.lst.append(dict(item))
        logging.info('Appended item: {0}'.format(item))
        return item

    def close_spider(self, spider):
        # send all scraped items to a CSV for processing by Dataflow
        df = pd.DataFrame(self.lst,
                          columns=['first_name',
                                   'last_name',
                                   'party',
                                   'state',
                                   'district'])
        df.to_csv(tmp_path)
        logging.info('Created CSV at {0}'.format(tmp_path))
        self.f.close()
        bucket = self.storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(tmp_path)
        logging.info('File {0} uploaded to {1}.'.format(
            tmp_path,
            gcs_path))
