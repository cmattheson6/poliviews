import scrapy
from scrapy.crawler import CrawlerProcess
from house_members.spiders.house_pols import HousePolsSpider

def main(data, context):
    process = CrawlerProcess()
    process.crawl(HousePolsSpider)
    process.start()
