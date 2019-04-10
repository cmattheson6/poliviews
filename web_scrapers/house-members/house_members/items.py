"""
This contains the Item build-out for each unit created by the Scrapy spider.
It creates the proper structure of the unit in order for successful upload to Google Pub/Sub.

"""

import scrapy


class HouseMembersItem(scrapy.Item):
    first_name = scrapy.Field()
    last_name = scrapy.Field()
    party = scrapy.Field()
    state = scrapy.Field()
    district = scrapy.Field()

