"""
This pipeline will upload all items from the spiders to the proper Pub/Sub topic.
The rest of the processing will take place in Dataflow.
"""


from google.cloud import pubsub
from google.oauth2 import service_account
import logging

class PoliticiansPipeline(object):
            
    def process_item(self, item, spider):
        """We need to establish a an authorized connection to Google Cloud in order to upload to Google Pub/Sub.
        In order to host the spiders on Github, the service account credentials are housed on the Scrapy platform
        and dynamically created in the script."""

        # Pull all of the credential info from the Scrapy platform into a dictionary.
        cred_dict = {
            "auth_provider_x509_cert_url": spider.settings.get('auth_provider_x509_cert_url'),
            "auth_uri": spider.settings.get('auth_uri'),
            "client_email": spider.settings.get('client_email'),
            "client_id": spider.settings.get('client_id'),
            "client_x509_cert_url": spider.settings.get('client_x509_cert_url'),
            "private_key": spider.settings.get('private_key'),
            "private_key_id": spider.settings.get('private_key_id'),
            "project_id": spider.settings.get('project_id'),
            "token_uri": spider.settings.get('token_uri'),
            "type": spider.settings.get('account_type')
        }
        logging.info('Credentials downloaded from Scrapy server.')
        cred_dict['private_key'] = cred_dict['private_key'].replace('\\n', '\n')
               

        # Build a Credentials object from the above dictionary. This will properly allow access as part of a
        # Google Cloud Client.
        credentials = service_account.Credentials.from_service_account_info(cred_dict)
        logging.info('Credentials object created.')

        # Create Publisher client.
        publisher = pubsub.PublisherClient(credentials = credentials)
        logging.info('Publisher Client created.')

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