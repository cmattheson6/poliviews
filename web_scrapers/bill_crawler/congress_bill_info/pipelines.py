"""
This pipeline will upload all items from the spiders to the proper Pub/Sub topic.
The rest of the processing will take place in Dataflow.

In order to handle two different types of dictionaries that need to go to two different topics, they need to be
handled accordingly:
 - There are two separate pipelines in the settings.py file: CongressBillInfoPipeline and BillCosponsorsPipeline
 - Check that the keys in the item exactly match the keys we are looking for.
 - If they check out, then it's good to go. If not, it gets passed to the other pipeline.

2)
"""
### -------- Import all necessary modules -------- ###
from google.cloud import pubsub
import logging
import pandas as pd
from datetime import date
import os
import sys
import unidecode
from subprocess import Popen

bill_file_dirname = '{0}/tmp/bill_info'.format(os.path.expanduser('~'))
bill_file_path = bill_file_dirname + '/bill_info_{0}.csv'.format(date.today())
rm_bill_files = 'rm {0}/*'.format(bill_file_dirname)
bill_cmd = Popen(rm_bill_files, shell=True).stdout.read()
print(bill_cmd)

cs_file_dirname = '{0}/tmp/cosponsors'.format(os.path.expanduser('~'))
cs_file_path = cs_file_dirname + '/cosponsors_{0}.csv'.format(date.today())
rm_cs_files = 'rm {0}/*'.format(cs_file_dirname)
cs_cmd = Popen(rm_cs_files, shell=True).stdout.read()
print(cs_cmd)

### -------- Start of the pipeline -------- ###

# This pipeline is only to uplaod the bill items
class CongressBillInfoPipeline(object):
    # Uploads bill information to the database
    f= open(bill_file_path, mode='a+')
    lst = []
    bill_info_keys = ['bill_id',
                      'amdt_id'
                      'bill_title',
                      'bill_summary',
                      'sponsor_fn',
                      'sponsor_ln',
                      'sponsor_party',
                      'sponsor_state',
                      'bill_url']
    def process_item(self, item, spider):
        # Filters out any cosponsor items and only uploads bill items.
        if all([i in self.bill_info_keys for i in item.keys()]):
            item = {k: unidecode.unidecode(v) for (k, v) in item.items()}
            self.lst.append(dict(item))
            logging.info('Appended item: {0}'.format(item))
            return item
        else:
            return item
    def close_spider(self, spider):
        # send all scraped items to a CSV for processing by Dataflow
        df = pd.DataFrame(self.lst, columns=self.bill_info_keys)
        df.to_csv(bill_file_path)
        logging.info('Created CSV at {0}'.format(bill_file_path))
        self.f.close()
# This pipeline is only to upload the bill cosponsors
class BillCosponsorsPipeline(object):
    # Uploads bill information to the database
    f= open(cs_file_path, mode='a+')
    lst = []
    cosponsor_keys = ['bill_id',
                      'amdt_id',
                      'cosponsor_fn',
                      'cosponsor_ln',
                      'cosponsor_party',
                      'cosponsor_state']
    # Builds and uploads query
    def process_item(self, item, spider):
        # Filters out any cosponsor items and only uploads bill items.
        if all([i in self.cosponsor_keys for i in item.keys()]):
            item = {k: unidecode.unidecode(v) for (k, v) in item.items()}
            self.lst.append(dict(item))
            logging.info('Appended item: {0}'.format(item))
            return item
        # If not true, sends the item to the next pipeline as-is.
        else:
            return item
    def close_spider(self, spider):
        # send all scraped items to a CSV for processing by Dataflow
        df = pd.DataFrame(self.lst, columns=self.cosponsor_keys)
        df.to_csv(cs_file_path)
        logging.info('Created CSV at {0}'.format(cs_file_path))
        self.f.close()