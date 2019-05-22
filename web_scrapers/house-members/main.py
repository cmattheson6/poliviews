import scrapy
from scrapy.crawler import CrawlerProcess
from house_members.spiders.house_pols import HousePolsSpider
import logging

logging.basicConfig(level=logging.INFO)

def main(data, context):
    process = CrawlerProcess()
    process.crawl(HousePolsSpider)
    process.start()
