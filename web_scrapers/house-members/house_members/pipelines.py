"""
This pipeline will upload all items from the spiders to the proper Pub/Sub topic.
The rest of the processing will take place in Dataflow.
"""


from google.cloud import pubsub
import logging
import pandas as pd

class PoliticiansPipeline(object):
    publisher = pubsub.PublisherClient()
    # def open_spider(self, spider):
    # set csv location and open it
    df = pd.DataFrame(columns=[]) #Figure out how to get the columns automatically populated
    # pass
    def process_item(self, item, spider):
        """We need to establish a an authorized connection to Google Cloud in order to upload to Google Pub/Sub.
        In order to host the spiders on Github, the service account credentials are housed on the Scrapy platform
        and dynamically created in the script."""

        # Set location of proper publisher topic
        project_id = 'politics-data-tracker-1'
        topic_name = 'house_pols'
        topic_path = self.publisher.topic_path(project_id, topic_name)
        data = u'This is a representative in the House.' #Consider how to better use this.
        data = data.encode('utf-8')
        future = self.publisher.publish(topic_path, data=data,
                          first_name = item['first_name'],
                          last_name = item['last_name'],
                          party = item['party'],
                          state = item['state'],
                          district = item['district'])
        logging.info('Published item: {0}'.format(future.result()))

        # Add the item as a row in the csv here
        self.df.append(dict(item)) #check this
        return item
    def close_spider(self, spider):
        # close spider
        self.df.to_csv() # figure out best location; tmp folder?
        pass