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
import re

file_dirname = '{0}/tmp/house_members'.format(os.path.expanduser('~'))
file_path = file_dirname + '/house_members_{0}.csv'.format(date.today())
rm_old_files = 'rm {0}/*'.format(file_dirname)
try:
    cmd = Popen(rm_old_files, shell=True).stdout.read()
    print(cmd)
except Exception as e:
    print(e)

class PoliticiansPipeline(object):
    # publisher = pubsub.PublisherClient()
    # set csv location and open it
    # file_path = '{0}/tmp/house_pols/house_pols_{1}.csv'.format(os.path.expanduser('~'), date.today())
    f= open(file_path, mode='a+')
    lst = []
    def process_item(self, item, spider):
        """We need to establish a an authorized connection to Google Cloud in order to upload to Google Pub/Sub.
        In order to host the spiders on Github, the service account credentials are housed on the Scrapy platform
        and dynamically created in the script."""

        # Set location of proper publisher topic
        # project_id = 'politics-data-tracker-1'
        # topic_name = 'house_pols'
        # topic_path = self.publisher.topic_path(project_id, topic_name)
        # data = u'This is a representative in the House.' #Consider how to better use this.
        # data = data.encode('utf-8')
        # future = self.publisher.publish(topic_path, data=data,
        #                   first_name = item['first_name'],
        #                   last_name = item['last_name'],
        #                   party = item['party'],
        #                   state = item['state'],
        #                   district = item['district'])
        # logging.info('Published item: {0}'.format(future.result()))

        # Add the item as a row in the csv here
        item = {k:unidecode.unidecode(v) for (k,v) in item.items()}
        item = {k: re.sub(',', '', v) for (k, v) in item.items()}
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