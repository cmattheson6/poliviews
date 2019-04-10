'''
This Scrapy pipeline will have a daily pull of the representatives in the Senate. This data comes
directly from the Senate website. This will be automated for daily pulls in order to keep the
current Senate member list up to date.
'''

import scrapy
import logging
from senate_members.items import SenateMembersItem


class GenSenateSpider(scrapy.Spider):
    name = 'gen_senate'
    
    # Sends a request to the senate page to pull current page's contents.
    def start_requests(self):
        start_url = 'http://www.senate.gov/general/contact_information/senators_cfm.xml'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
        yield scrapy.Request(url = start_url, callback = self.parse, headers = headers)
            
    # Parse fucntion pulls the needed data on Senators out of the page.
    def parse(self, response):
        # This site has ~ a 50% success rate of connecting. This ensures to keep attempting to re-connect
        # until it successfully connects.
        if response.status == 403:
            logging.info('Couldn\'t successfully connect. Retrying . . . ')
            start_url = 'http://www.senate.gov/general/contact_information/senators_cfm.xml'
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36'}
            yield scrapy.Request(url = start_url, callback = self.parse, headers = headers);
        else:
            logging.info('Successfully connected.')
            logging.info('{0}'.format(response.status))
            pass;
        # Establishes path to each representative and their block of information.
        for i in response.xpath("./member"):
            # Extract the senator's information.
            first_name = i.xpath("./first_name/text()").extract_first()
            last_name = i.xpath("./last_name/text()").extract_first()
            party = i.xpath("./party/text()").extract_first()
            state = i.xpath("./state/text()").extract_first()

            # Combine all parts of the senator into an Item for proper upload.
            senator_item = SenateMembersItem()
            senator_item['first_name'] = first_name
            senator_item['last_name'] = last_name
            senator_item['party'] = party
            senator_item['state'] = state
            logging.info('New Senator: {0}'.format(senator_item))
            # yields the Item, which will then get sent to the Scrapy pipeline to send to Pub/Sub.
            yield senator_item;
