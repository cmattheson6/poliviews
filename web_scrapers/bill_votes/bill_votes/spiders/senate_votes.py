'''
This Scrapy pipeline will have a daily pull of any votes on bills in the Senate. This data comes
directly from the Senate website. This will be automated for daily pulls in order to keep the
current voting lists up-to-date.
'''


### -------- Import all of the necessary files -------- ###
import scrapy
from datetime import datetime, timedelta
from datetime import date
import unidecode

### -------- Define all custom fxns here -------- ###

# This formula is used to properly format the bills to enusre that there is naming normalization across platforms.
def clean_bill(x):
    bill = x.replace(" ", "").replace(".", "").upper()
    return bill

# Set date for yesterday's bills that are published
date_yesterday = date.today() - timedelta(days=1)
this_year = date_yesterday.year

# Automates the changing of the URLs every year.
congress_num = ((this_year - 1789)/2) + 1
session_num = 2 if this_year % 2 == 0 else 1

### -------- Start of spider -------- ###

class SenateVotesSpider(scrapy.Spider):
    name = 'senate_votes'

    # Sends a request to the senate page to pull current page's contents.
    def start_requests(self):
        start_urls = ['https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_{0}_{1}.htm'
                          .format(congress_num, session_num)]
        # Start the parsing request
        for u in start_urls:
            yield scrapy.Request(url = u, callback = self.parse_all_bills)
    # This request contains links to all bills and their corresponding votes.
    def parse_all_bills(self, response):
        all_bill_urls = []
        for i in response.xpath(".//table/tr"):
            # Pull bill URL
            bill_url = i.xpath(".//td/a/@href").re_first("^.*vote=.*$")
            # Check the date of the URL
            bill_date = i.xpath(".//td/text()").extract()
            bill_date = bill_date[len(bill_date)-1]
            bill_date = unidecode.unidecode(bill_date)
            bill_date = bill_date + ", " + str(date_yesterday.year)
            bill_date = datetime.strptime(bill_date, "%b %d, %Y")
            bill_date = bill_date.date()

            if bill_date > date_yesterday:
                pass
            elif bill_date == date_yesterday:
                all_bill_urls.append(bill_url)
            else:
                all_bill_urls.append(bill_url) #CHANGE THIS TO BREAK WHEN I ONLY WANT THE MOST CURRENT BILLS
        # All bill vote links will be passed to this request to pull out the votes for each individual bill.
        for u in all_bill_urls:
            url = response.urljoin(u)
            print(url)
            yield scrapy.Request(url = url, callback = self.parse_bill)
    # This parse is simply a redirect to the XML version of the file. It is easier to pull out the vote info on that
    # version of the page.
    def parse_bill(self, response):
        bill_xml = response.xpath(".//span[@style='float: right']/a/@href").extract_first()
        bill_xml = response.urljoin(bill_xml)
        yield scrapy.Request(url = bill_xml, callback = self.parse_votes)
    # This request will extract all of the votes for a singular bill. The bill's URL has still been retained
    # for future purposes.
    def parse_votes(self, response):
        try:
            bill_id = clean_bill(response.xpath(".//document/document_name/text()").extract_first())
        except Exception:
            bill_id = clean_bill(response.xpath(".//amendment/amendment_to_document_number/text()").extract_first())
        else:
            raise ValueError('Unable to parse the bill number.')
        # Pull vote date and process it for proper formatting
        vote_date = response.xpath(".//vote_date/text()").extract_first()
        vote_date = datetime.strptime(vote_date, "%B %d, %Y, %I:%M %p")
        vote_date = vote_date.date()
        # Parse amendment number
        try:
            amdt_id = clean_bill(response.xpath(".//amendment/amdt_idber/text()").extract_first())
        except Exception:
            amdt_id = None
#         else:
#             raise ValueError
        if amdt_id == "":
            amdt_id = None
        # See if I need any exceptions or conditions w/ test
        for i in response.xpath(".//member"):
            # Parse first name of one rep's vote.
            fname = i.xpath(".//first_name/text()").extract_first()
            # Parse last name of one rep's vote.
            lname = i.xpath(".//last_name/text()").extract_first()
            # All states parse
            state = i.xpath(".//state/text()").extract_first()
            # Parse state of one rep's vote.
            party = i.xpath(".//party/text()").extract_first()
            # Parse party of one rep's vote.
            vote_cast = i.xpath(".//vote_cast/text()").extract_first()
            # Repurpose Yea as 1, Nay as 0, anything else as None
            if vote_cast == "Yea":
                vote_cast = 1
            elif vote_cast == "Nay":
                vote_cast = 0
            else:
                vote_cast = None
            # Build out an individual dict for each vote and yield that
            vote_dict = {'bill_id': bill_id,
                         'amdt_id': amdt_id,
                         'first_name': fname,
                         'last_name': lname,
                         'party': party,
                         'state': state,
                         'vote_cast': vote_cast,
                         'vote_date': vote_date,
                         'chamber': 'SN',
                         'chamber_state': 'US'}
            # Send the votes to enter the pipeline.
            yield vote_dict
             
    
