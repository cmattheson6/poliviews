import scrapy
from scrapy.crawler import CrawlerProcess
from house_members.spiders.house_pols import HousePolsSpider
from house_members.settings import house_members_settings
import logging
from google.cloud import storage
import google.cloud.logging as logger
from house_members.pipelines import tmp_path
from datetime import date

def main(data, context):
    pass

if __name__ == '__main__':
    main(data=None, context=None)

