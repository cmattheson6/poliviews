"""
This pipeline will upload all items from the spiders to the proper Pub/Sub topic.
The rest of the processing will take place in Dataflow.
"""


from google.cloud import pubsub
import logging

# Create Publisher client.
publisher = pubsub.PublisherClient()
logging.info('Publisher Client created.')

class PoliticiansPipeline(object):
            
    def process_item(self, item, spider):
        """We need to establish a an authorized connection to Google Cloud in order to upload to Google Pub/Sub.
        In order to host the spiders on Github, the service account credentials are housed on the Scrapy platform
        and dynamically created in the script."""

        # Set location of proper publisher topic
        project_id = 'politics-data-tracker-1'
        topic_name = 'house_pols'
        topic_path = publisher.topic_path(project_id, topic_name)
        data = u'This is a representative in the House.' #Consider how to better use this.
        data = data.encode('utf-8')
        publisher.publish(topic_path, data=data,
                          first_name = item['first_name'],
                          last_name = item['last_name'],
                          party = item['party'],
                          state = item['state'],
                          district = item['district'])
        logging.info('Published item: {0}'.format(item))
        return item