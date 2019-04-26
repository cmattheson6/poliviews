"""
This pipeline will upload all items from the spiders to the proper Pub/Sub topic.
The rest of the processing will take place in Dataflow.
"""

from google.cloud import pubsub
import logging
import pandas as pd
from datetime import date
import os
import sys
import unidecode
from subprocess import Popen

file_dirname = '{0}/tmp/bill_votes'.format(os.path.expanduser('~'))
file_path = file_dirname + '/bill_votes_{0}.csv'.format(date.today())
rm_old_files = 'rm {0}/*'.format(file_dirname)
cmd = Popen(rm_old_files, shell=True).stdout.read()
print(cmd)

class BillVotesPipeline(object):

    f= open(file_path, mode='a+')
    lst = []

    def process_item(self, item, spider):
        """We need to establish a an authorized connection to Google Cloud in order to upload to Google Pub/Sub.
        In order to host the spiders on Github, the service account credentials are housed on the Scrapy platform
        and dynamically created in the script."""
        item = {k: unidecode.unidecode(v) for (k, v) in item.items()}
        self.lst.append(dict(item))
        logging.info('Appended item: {0}'.format(item))
        return item

    def close_spider(self, spider):
        # send all scraped items to a CSV for processing by Dataflow
        df = pd.DataFrame(self.lst,
                          columns=['bill_id',
                                   'amdt_id',
                                   'first_name',
                                   'last_name',
                                   'party',
                                   'state',
                                   'vote_cast',
                                   'vote_date',
                                   'chamber',
                                   'chamber_state'])
        df.to_csv(file_path)
        logging.info('Created CSV at {0}'.format(file_path))
        self.f.close()
