import scrapy
from scrapy.crawler import CrawlerProcess
from house_members.spiders.house_pols import HousePolsSpider
import logging

logging.basicConfig(level=logging.INFO)

def main(data, context):
    logging.info('Started main function.')
    process = CrawlerProcess()
    logging.info('Initiated CrawlerProcess.')
    process.crawl(HousePolsSpider)
    process.start()
    logging.info('Start HousePolsSpider crawl.')
