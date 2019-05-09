import scrapy
from scrapy.crawler import CrawlerProcess
from house_members.spiders.house_pols import HousePolsSpider

def scrapy_test():
    process = CrawlerProcess()
    process.crawl(HousePolsSpider)
    process.start()
