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

project_id = 'politics-data-tracker-1'
bucket_name = 'poliviews'
pipeline_name = 'house_members'
file_dirname = 'gs://{0}/{1}/csvs/{2}'.format(project_id, bucket_name, pipeline_name)
file_path = file_dirname + '/{0}_{1}.csv'.format(date.today())
rm_old_files = 'rm {0}/*'.format(file_dirname)
try:
    cmd = Popen(rm_old_files, shell=True).stdout.read()
    print(cmd)
except Exception as e:
    print(e)

class PoliticiansPipeline(object):
    # set csv location and open it\
    f= open(file_path, mode='a+')
    lst = []
    def process_item(self, item, spider):
        """We need to establish a an authorized connection to Google Cloud in order to upload to Google Pub/Sub.
        In order to host the spiders on Github, the service account credentials are housed on the Scrapy platform
        and dynamically created in the script."""

        # Add the item as a row in the csv here
        item = {k:unidecode.unidecode(v) for (k,v) in item.items()}
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
        df.to_csv(file_path)
        logging.info('Created CSV at {0}'.format(file_path))
        self.f.close()