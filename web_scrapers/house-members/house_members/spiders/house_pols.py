'''
This Scrapy pipeline will have a daily pull of the representatives in the House of Representatives. This data comes
directly from the House of Representatives website. This will be automated for daily pulls in order to keep the
current House member list up to date.
'''

import scrapy
import logging
from house_members.items import HouseMembersItem

class HousePolsSpider(scrapy.Spider):
    name = "house_pols"

    # Sends a request to the senate page to pull current page's contents.
    def start_requests(self):
        start_url = "https://www.house.gov/representatives"
        yield scrapy.Request(url = start_url, callback = self.parse)

    # Parse function pulls the needed data on Senators out of the page.
    def parse(self, response):
        
        # finds the path to the table that contains a single state's representatives
        for region in response.xpath("//table[@class='table']"):
            # filters out any non-state tables
            if "state" not in region.xpath(".//caption/@id").extract_first():
                continue
            else: 
                pass
            # pulls the state of all representatives in this group; will be added back in.
            state = region.xpath(".//caption/text()").extract_first()
            state = state.strip(' \t\n\r')
            
            # establishes the path to each representative of the one state.
            for rep in region.xpath(".//tbody/tr"):
                full_name = rep.xpath(".//td/a/text()").extract_first()
                
                # pulls the first and last name of the representative.
                if "," in full_name:
                    first_name = full_name.split(',')[1]
                    last_name = full_name.split(',')[0]
                else:
                    full_name = full_name.replace(" ", ",", 1)
                    first_name = full_name.split(',')[1]
                    last_name = full_name.split(',')[0]
                first_name = first_name.strip(' \t\n\r')
                last_name = last_name.strip(' \t\n\r')
                
                # pulls the party of the given representative
                party = rep.xpath(".//td/text()").extract()[2]
                party = party.strip(' \t\n\r')
                
                # pulls the district of the given representative.
                district = rep.xpath(".//td/text()").extract()[0]
                district = district.strip(' \t\n\r')

                # Combine all parts of the representative into an Item for proper upload.
                house_item = HouseMembersItem()
                house_item['first_name'] = first_name
                house_item['last_name'] = last_name
                house_item['party'] = party
                house_item['state'] = state
                house_item['district'] = district
                logging.info('New Rep: {0}'.format(house_item))
                # yields the Item, which will then get sent to the Scrapy pipeline to send to Pub/Sub.
                yield house_item;
