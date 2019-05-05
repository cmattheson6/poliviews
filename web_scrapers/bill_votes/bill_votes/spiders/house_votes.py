'''
This Scrapy pipeline will have a daily pull of any votes on bills in the House of Representatives. This data comes
directly from the House of Representatives website. This will be automated for daily pulls in order to keep the
current voting lists up-to-date.
'''

from datetime import datetime, timedelta
from datetime import date
import logging
import scrapy

# Set date for yesterday's bills that are published
date_yesterday = date.today() - timedelta(days=1)
# Sets the year of yesterday's bills
this_year = date_yesterday.year

# This formula is used to properly format the bills to enusre that there is naming normalization across platforms.
def clean_bill(x):
    bill = x.replace(" ", "").replace(".", "").upper()
    return bill

class HouseVotesSpider(scrapy.Spider):
    name = 'house_votes'

    # Sends a request to the senate page to pull current page's contents.
    def start_requests(self):
        start_urls = ['http://clerk.house.gov/evs/{0}/index.asp'.format(this_year)]

        # Starts the request for all bill votes.
        for u in start_urls:
            yield scrapy.Request(url = u, callback = self.parse_all_bills)

    # The index page does not contain all of the bills. The "ROLL" links at the bottom of the index page contains all
    # bills for the year.
    def parse_all_bills(self, response):
        # Pull all links to a single bill's votes from this page
        all_roll_urls = response.xpath('.//a/@href').re('^.*ROLL.*$')
        for u in all_roll_urls:
            # Ensure we are getting the full URLs.
            url = response.urljoin(u)
            # Pass all links to the next parsing request
            yield scrapy.Request(url = url, callback = self.parse_roll_call)

    # This parse function's goal is to get all of the links to a single bill's votes and the link to the Congress
    # bill summary page.
    def parse_roll_call(self, response):
        roll_call_votes = response.xpath(".//table/tr")
        roll_call_votes = roll_call_votes[1:len(roll_call_votes)]
        bill_vote_dicts = []
        for i in roll_call_votes:
            # Pulls the link to the actual votes.
            vote_url = i.xpath(".//a/@href").re_first("^.*rollnumber.*$")
            logging.info('Currently crawling bill: {0}'.format(vote_url)) #MOVE THIS
            vote_url = response.urljoin(vote_url)
            # Pull the bill's url on congress.gov to get any amendment info.
            bill_url = i.xpath(".//a/@href").re_first("^.*congress.gov.*$")
            bill_url = response.urljoin(bill_url) + "/amendments?pageSort=asc"
            bill_date = i.xpath(".//text()").extract()[2]
            bill_date = bill_date + ", " + str(date.today().year)
            bill_date = datetime.strptime(bill_date, "%d-%b, %Y").date()
            # This will only pull in any votes that occurred yesterday otherwise it will be ignored.
            if bill_date > date_yesterday:
                pass
            if bill_date == date_yesterday:
                # This request will link to the list of votes and make sure to retain the bill's congress.gov URL link.
                request = scrapy.Request(url = vote_url, 
                                 callback = self.parse_votes)
                request.meta['bill_url'] = bill_url
                yield request
            else: #CHANGE THIS TO 'BREAK' WHEN ALL OF THIS YEAR HAS BEEN PROPERLY UPLOADED
                # break
                request = scrapy.Request(url = vote_url, 
                                 callback = self.parse_votes)
                request.meta['bill_url'] = bill_url
                yield request

    # This request will extract all of the votes for a singular bill. The bill's URL has still been retained
    # for future purposes.
    def parse_votes(self, response):
        # Sets bill URL
        bill_url = response.meta['bill_url']
        # The page is structured differently whether it was a vote on an amendment or just on the bill.
        # This accounts for that.
        try:
            bill_id = clean_bill(response.xpath(".//legis-num/text()").extract_first())
        except Exception:
            bill_id = clean_bill(response.xpath(".//amendment/amendment_to_document_number/text()").extract_first())
        else:
            raise ValueError('Not receiving bill ID from {0}'.format(response))
        if response.xpath(".//amendment-num/text()").extract_first() == None:
            amdt_num = None
        elif len(response.xpath(".//amendment-num/text()").extract()) == 1:
            amdt_num = int(response.xpath(".//amendment-num/text()").extract_first())
        else:
            raise ValueError('Error parsing amendment from {0}'.format(response))

        # Will be updated in later parse functions if there is an amendment
        amendment_id = None
        # Pull vote date and process it for proper formatting.
        vote_date = response.xpath(".//action-date/text()").extract_first()
        vote_date = datetime.strptime(vote_date, "%d-%b-%Y")
        vote_date = vote_date.date()
        # This list will contain all vote's dictionaries. This will be uploaded as-is if there is no amendment.
        # The list will need to be passed as a meta variable to the amendment parse function if there is an amendment.
        vote_list = []
        for i in response.xpath(".//recorded-vote"):
            # Parse last name of one rep's vote.
            last_name = i.xpath(".//legislator/@unaccented-name").extract_first()
            # Parse state of one rep's vote.
            state = i.xpath(".//legislator/@state").extract_first()
            # Parse party of one rep's vote.
            party = i.xpath(".//legislator/@party").extract_first()
            # Parse out vote that the rep cast on the bill.
            vote_cast = i.xpath(".//vote/text()").extract_first()
            # Refactor Yea as 1, Nay as 0, anything else as None
            if vote_cast == "Aye":
                vote_cast = 1
            elif vote_cast == "No":
                vote_cast = 0
            else:
                vote_cast = None
            # Build out an individual dict for each vote and yield that
            vote_dict = {'bill_id': bill_id,
                         'amendment_id': amendment_id,
                         'first_name': None,
                         'last_name': last_name,
                         'state': state,
                         'party': party,
                         'vote_cast': vote_cast,
                         'vote_date': vote_date,
                         'chamber': 'HR',
                         'chamber_state': 'US'}
            vote_list.append(vote_dict)
        # If there is no amendment, the votes enter the pipeline.
        # If there is an amendment, send all votes to the parse_amendment request to get all amendment information
        if amdt_num == None:
            logging.info('Bill votes being uploaded from {0}'.format(response))
            for i in vote_list:
                yield i
        elif type(amdt_num) == int:
            request = scrapy.Request(url = bill_url,
                                     callback = self.parse_amendment,
                                     dont_filter = True)
            request.meta['amdt_num'] = amdt_num
            request.meta['vote_list'] = vote_list
            logging.info('Redirecting to bill url {0}'.format(bill_url))
            yield request
        else:
            raise ValueError('Amendment not processed correctly from {0}'.format(response))
    # This request will parse the amendment ID and append it to the previously-generated vote list.
    # This is only used for bills that have amendments.
    def parse_amendment(self, response):
        amdt_num = response.meta['amdt_num']
        vote_list = response.meta['vote_list']
        # Extracts all amendment IDs on the page.
        all_amdts = [i.xpath(".//a/text()").extract_first() \
                     for i in response.xpath(""".//ol[@class='basic-search-results-lists expanded-view']/li[@class='expanded']
                     /span[@class='result-heading amendment-heading']""")]
        # A page only contains 100 amendments. This pulls the link to the next page of amendments if needed
        # for a new request.
        amdt_next_pg = response.xpath(".//div[@class='pagination']/a[@class='next']/@href").extract_first()
        if (amdt_num >= 100) and (amdt_next_pg != None):
            amdt_next_pg = response.urljoin(amdt_next_pg)
            amdt_num = amdt_num - 100
            # This will loop the request until we reach the proper amendment page.
            request = scrapy.Request(url = amdt_next_pg,
                                 callback = self.parse_amendment,
                                 dont_filter = True)
            request.meta['amdt_num'] = amdt_num
            request.meta['vote_list'] = vote_list
            yield request
        else:
            # Pull the proper ID, add it to each dictionary in the vote list, and send to the Pub/Sub.
            amdt_id = all_amdts[amdt_num-1]
            amdt_id = clean_bill(amdt_id)
            for i in vote_list:
                i['amendment_id'] = amdt_id
            for i in vote_list:
                yield i


    
