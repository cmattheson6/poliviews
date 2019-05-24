import scrapy
from scrapy.crawler import CrawlerProcess
from house_members.spiders.house_pols import HousePolsSpider
from house_members.settings import house_members_settings
import logging

logging.basicConfig(level=logging.INFO)

def main(data, context):
    process = CrawlerProcess(settings=house_members_settings)
    logging.info('Initiated CrawlerProcess.')
    process.crawl(HousePolsSpider)
    process.start()
    logging.info('Start HousePolsSpider crawl.')

if __name__ == '__main__':
    main()

