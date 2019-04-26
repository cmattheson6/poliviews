'''
This Scrapy pipeline will have a daily pull of the current day's bills that are presented to Congress. This data comes
directly from the Congressional website. This will be automated for daily pulls in order to keep the
current bill list up to date.
'''

### -------- Import all of the necessary files -------- ###
import scrapy
import re
from datetime import datetime, timedelta
from datetime import date

### -------- Define all custom fxns here -------- ###

# Find the location of needed character in a string.
# Nothing in the 're' package does exactly what was needed for this fxn.
def find_character(s, ch):
    index_nums = []
    index = 0
    for x in s:
        if x == ch:
            index_nums.append(index)
            index = index + 1
        else:       
            index = index + 1
    return index_nums

def create_pol_dict(pol):
    regex = '[^a-zA-Z]'
    pol_fn = pol[pol.index(",")+1:pol.index("[")].strip()
    pol_ln = pol[pol.index(" ")+1:pol.index(",")]
    s = pol[pol.index("["):pol.index("]")+1]
    pol_party = s[s.index("[")+1:s.index("-")]
    if s.count('-') == 2:
        pol_state = s[find_character(s, '-')[0]+1:find_character(s, '-')[1]]
    elif s.count('-') == 1:
        pol_state = s[find_character(s, '-')+1:len(s)]
    else:
        raise IndexError('String formatting does not fit pre-defined formats.')
    pol_dict = {'first_name': pol_fn,
                'last_name': pol_ln,
                'party': pol_party,
                'state': pol_state}
    return pol_dict
def create_bill_dict(b): #inputs raw bill info
    if b.count('-') > 0:
        bill_id = b[0:b.index("-")].replace(" ", "").replace(".", "")
        amdt_id = None
        bill_title = b[b.index("-")+1:len(b)].strip()
        bill_dict = {'bill_id': bill_id,
                     'amdt_id': amdt_id,
                     'bill_title': bill_title}
        return bill_dict
    else: 
        bill_id = b[b.index("to")+1:len(b)].strip()
        amdt_id = b[0:b.index("to")].replace(" ", "").replace(".", "")
        bill_title = b
        bill_dict = {'bill_id': bill_id,
                     'amdt_id': amdt_id,
                     'bill_title': bill_title}
        return bill_dict


# Set date for yesterday's bills that are published
date_yesterday = date.today() - timedelta(days=1)
this_year = date_yesterday.year

# Automates the changing of the URLs every year.
congress_num = ((this_year - 1789)/2) + 1
session_num = 2 if this_year % 2 == 0 else 1

### -------- Start of the spider -------- ###

class BillCrawlerSpider(scrapy.Spider):
    # Set the name of the spider and the corresponding starting points of the scrape
    name = 'bill_crawler'
    allowed_domains = ['www.congress.gov']
    start_urls = ['''https://www.congress.gov/search?q=%7B%22congress%22%3A%22{0}%22%
                  2C%22source%22%3A%22legislation%22%7D&searchResultViewType=expanded&page=1'''
                      .format(congress_num)]

    # The first parse will pull all URLs from the page that link to individual bills.
    def parse(self, response):
        bill_table = response.xpath('.//li[@class="expanded"]')
        for i in bill_table:
            # Pulls and formats the bill date.
            bill_date = i.xpath('.//span[@class="result-item"/text()').extract()[2]
            bill_date = datetime.strptime(bill_date, ' (Introduced %m/%d/%Y) ')
            bill_date = bill_date.date()

            # Pulls and formats the bill's URL
            bill_url = i.xpath('.//a/@href').re_first(r'^.*/bill/.*/.*/[0-9]*\?.*$')
            bill_url = bill_url[0:re.search(r'\?', bill_url).start()]

            # The loop should stop here based on if the date of a bill was not from yesterday.
            if bill_date > date_yesterday:
                pass
            elif bill_date == date_yesterday:
                yield scrapy.Request(url = bill_url,
                                     callback = self.parse_bill)
            else:
                yield scrapy.Request(url = bill_url,
                                     callback = self.parse_bill) # Change to break after first pass at scraping site.
        
        # Because of the set-up of the site,
        # this section should only be used once to build a full database the first time
        # Determine if there is a next page link, what that link is
        next_page = response.xpath(".//a[@class='next']/@href").extract_first()
        
        # Follow the next_page link from the top of the spider if available
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(url = next_page,
                                 callback = self.parse)

    # The second parse will pull out all of the bill's relevant information
    def parse_bill(self, response):
        # Parses out all of the bill info from the page.
        bill_info = response.xpath(".//h1[@class='legDetail']/text()").extract_first()
        bill_info = create_bill_dict(bill_info)

        bill_id = bill_info['bill_id']
        amdt_id = bill_info['amdt_id']
        bill_title = bill_info['bill_title']

        bill_subtitle = response.xpath(".//table[@class='standard01']/tr/td/text()").extract_first()
        bill_title = bill_title + bill_subtitle

        # Get all sponsor info parsed from raw info
        sponsor_info = response.xpath(".//table[@class='standard01']/tr/td/a/text()").re_first(r'^.*\[.*\]$')
        sponsor_info = create_pol_dict(sponsor_info)

        # Parse the summary description of the bill.
        bill_summary = response.xpath(".//div[@id='bill-summary']/p/text()").extract_first()


        # Assign all parts of the bill to a dictionary for pipeline discovery        
        bill_dict = {'bill_id': bill_id, 
                     'amdt_id': amdt_id,
                     'bill_title': bill_title, 
                     'bill_summary': bill_summary,
                     'sponsor_fn': sponsor_info['first_name'],
                     'sponsor_ln': sponsor_info['last_name'],
                     'sponsor_party': sponsor_info['party'],
                     'sponsor_state': sponsor_info['state'],
                     'bill_url': response.request.url}
                
        #Pull the link to the cosponsors of the bill.
        url = response.xpath(".//a[contains(text(), 'Cosponsors')]/@href").extract_first()
        url = response.urljoin(url)
        cosponsors_request = scrapy.Request(url = url,
                                            callback = self.parse_cosponsors)
        # This generator needs to yield both the bill and a separate cosponsor request
        # The two scrapes need to be separate     
        yield bill_dict
        yield cosponsors_request
    
    # Generates all cosponsors of the bill for a secondary dataset. Both this and the original dictionary will
    # flow and be filtered through the same pipeline.
    def parse_cosponsors(self, response):
        # Parses out all cosponsor information
        bill_info = response.xpath(".//h1[@class='legDetail']/text()").extract_first()  
        bill_info = create_bill_dict(bill_info)
        cosponsors_info = response.xpath(".//table[@class = 'item_table']/tbody/tr/td/a/text()").extract()
        bill_id = bill_info['bill_id']
        amdt_id = bill_info['amdt_id']

        # Parse out all cosponsors of a bill.
        cosponsors_info = (create_pol_dict(pol) for pol in cosponsors_info)
        # There may be multiple cosponsors, so a dictionary is generated for each cosponsor
        # then uploaded individually.
        for pol in cosponsors_info:
            cosponsor_dict = {'bill_id': bill_id,
                              'amdt_id': amdt_id,
                              'cosponsor_fn': pol['first_name'],
                              'cosponsor_ln': pol['last_name'],
                              'cosponsor_party': pol['party'],
                              'cosponsor_state': pol['state'],
                             }
            yield cosponsor_dict