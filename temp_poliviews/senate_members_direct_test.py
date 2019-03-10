# -------- Import all necessary modules -------- #
# Get the logging set for debugging
import logging
import os

# Pull in all Pubsub, Dataflow, and BigQuery modules
from google.cloud import pubsub
from google.cloud import bigquery

# all Apache modules needed
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import poliviews.transforms.df_ptransforms as pt
from apache_beam.testing.test_pipeline import TestPipeline

# Date modules for date formatting
from datetime import date as dt

# Miscellaneous modules

test_senator_old = {
    'pol_fn': 'Cameron',
    'pol_ln': 'Mattheson',
    'pol_party': 'R',
    'pol_state': 'MA'
}
test_senator_new = {
    'pol_fn': 'Carol',
    'pol_ln': 'Brantley IV',
    'pol_party': 'R',
    'pol_state': 'FL'
}

logging.basicConfig(level=logging.INFO)

n_tbl_ex = [{'nickname': 'Joe', 'full_name': 'Joseph'},
            {'nickname': 'Carol', 'full_name': 'Caroline'}]
pol_tbl_ex = [{'first_name': 'Cameron',
               'last_name': 'Mattheson',
               'party': 'R',
               'state': 'MA',
               'suffix': None,
               'nickname': None}]

logging.info('Created test dataset.')

project_id = 'politics-data-tracker-1'
topic_name = 'senate_pols'
subscription_name = 'senate_pols_df'

dataset_id = 'poliviews'
senate_table_id = 'senate'
pol_table_id = 'politicians'
nickname_table_id = 'nicknames'

pol_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, pol_table_id)
senate_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, senate_table_id)

x_data = u'This is a senator in the Senate.'
x_data = x_data.encode('utf-8')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:\Users\cmatt\PycharmProjects\dataflow_scripts\poliviews\gcp_credentials.txt'
# ptransform_file = 'C:\Users\cmatt\PycharmProjects\dataflow_scripts\df_ptransforms.py'
publisher = pubsub.PublisherClient()

logging.info('Publisher client constructer.')

topic_path = publisher.topic_path(project_id, topic_name)
future = publisher.publish(topic_path,
                           data=x_data,
                           pol_fn=test_senator_old['pol_fn'],
                           pol_ln=test_senator_old['pol_ln'],
                           pol_party=test_senator_old['pol_party'],
                           pol_state=test_senator_old['pol_state'])

logging.info('Published message ID {0}'.format(future.result()))

future = publisher.publish(topic_path,
                           data=x_data,
                           pol_fn=test_senator_new['pol_fn'],
                           pol_ln=test_senator_new['pol_ln'],
                           pol_party=test_senator_new['pol_party'],
                           pol_state=test_senator_new['pol_state'])

logging.info('Published message ID {0}'.format(future.result()))

# subscriber = pubsub.SubscriberClient()
# subscription_path = subscriber.subscription_path(project_id, subscription_name)
#
#
# def callback(message):
#     print('Received message: {}'.format(message))
#     message.ack()
#
#
# subscriber.subscribe(subscription_path, callback=callback)

### DELETE STARTS HERE

# This DoFn pulls out the attributes of a PubSubMessage into a dictionary.
class IsolateAttrFn(beam.DoFn):
    def process(self, element):
        logging.info('Element is now type {0} before IsolateAttrFn.'.format(type(element)))
        attr_dict = element.attributes
        logging.info('Element is now type {0} after IsolateAttrFn.'.format(type(attr_dict)))
        yield attr_dict

# All data points come in as unicode. This converts all non-string data points into the proper data type.
# In this case, it is just the vote that needs to be converted to an integer.
class FixTypesFn(beam.DoFn):
    def process(self, element):
        logging.info('The element is type {0} before FixTypesFn.'.format(type(element)))
        d = {str(k):str(v) for (k, v) in element.items()}
        d['vote_cast'] = int(float(d['vote_cast']))
        logging.info('The element is  type {0} after FixTypesFn.'.format(type(d)))
        yield d

class MkStringsFn(beam.DoFn):
    def process(self, element):
        logging.info('The element is type {0} before FixTypesFn.'.format(type(element)))
        d = {str(k):str(v) for (k, v) in element.items()}
        logging.info('The element is  type {0} after FixTypesFn.'.format(type(d)))
        yield d

# In order to map properly, we need to reduce all names to a uniform unit.
# The next two formulas will filter, alter, and output as close to that as possible
# and make it a 'pure' representation of a politician's name.

# def check_suffix(s, keep_suffix = False):
#     match = re.search(r' (Sr.|Jr.|III|IV)', s)
#     if match:
#         if keep_suffix == True:
#             suffix = s[match.start():match.end()].strip()
#         s = re.sub(r' (Sr.|Jr.|III|IV)', '', s)
#         return s, suffix
#     else:
#         suffix = None
#         return s, suffix

# def scrub_name(s, keep_suffix = False):
#     if len(re.findall(r'[A-Z]\.', s)) == 1:
#         s = re.sub(r' [A-Z]\.', '', s)  # remove middle initials
#     s = re.sub(r' \".*\"', '', s)  # remove nicknames
#     s, suffix = check_suffix(s, keep_suffix=keep_suffix)
#     s = re.sub(r' \(.*\)', '', s)  # remove any parentheses
#     s = re.sub(r'\,', '', s)  # remove stray commas
#     s = s.strip()  # remove excess whitespace
#     return s, suffix

class ScrubFnameFn(beam.DoFn):
    def process(self, element, keep_suffix):
        logging.info('This element is type {0} before ScrubFnameFn.'.format(type(element)))
        s = element['pol_fn']
        s, suffix = pt.scrub_name(s, keep_suffix)
        element['pol_fn'] = s
        element['suffix'] = suffix
        logging.info('This element is type {0} after ScrubFnameFn.'.format(type(element)))
        yield element

# Same purpose as above but for last names.
class ScrubLnameFn(beam.DoFn):
    def process(self, element):
        logging.info('This element is type {0} before ScrubLnameFn.'.format(type(element)))
        s = element['pol_ln']
        if element['suffix'] == None:
            s, suffix = pt.scrub_name(s, keep_suffix=True)
            element['suffix'] = suffix
        else:
            s = pt.scrub_name(s, keep_suffix=False)
        element['pol_ln'] = s
        logging.info('This element is type {0} after ScrubLnameFn.'.format(type(element)))
        print(element)
        yield element

# If a politician's name is referenced multiple ways in different sources, this will
# simplify the two data sources to a uniform descriptor. It references a BigQuery table
# that we keep updated based on any discrepancies we run into.
class FixNicknameFn(beam.DoFn):
    def process(self, element, n_tbl, keep_nickname=False, nickname=None):
        fn = element['pol_fn']
        n_list=[]
        for i in n_tbl:
            if i['nickname']==fn:
                n_list.append(i)
            else:
                pass
        if len(n_list)==1:
            nickname=fn
            fn=n_list[0]['full_name']
        if len(n_list)>1:
            SyntaxError('There are multiple references of the nickname {0} for element {1}.'.format(fn, element))
        else:
            pass
        element['pol_fn'] = fn
        if keep_nickname == True:
            element['nickname'] = nickname
        yield element

### DELETE STOPS HERE

options = PipelineOptions(streaming=True,
                          runner='DataflowRunner',
                          project=project_id,
                          temp_location='gs://{0}/tmp'.format(project_id),
                          staging_location='gs://{0}/staging'.format(project_id))
                          # extra_package=ptransform_file)

p = TestPipeline(options=options)
logging.info('Created DirectRunner pipeline.')


class ReformatKeysFn(beam.DoFn):
    def process(self, element):
        element['first_name']=element.pop('pol_fn')
        element['last_name']=element.pop('pol_ln')
        element['party']=element.pop('pol_party')
        element['state']=element.pop('pol_state')
        print(element)
        yield element

# NEED A BIGQUERY FXN HERE TO PULL IN THE POLITICIANS TABLE

client = bigquery.Client()
nickname_query = client.query("""
    select * from `{0}.{1}.{2}`""".format(project_id, dataset_id, nickname_table_id))
nickname_tbl = nickname_query.result()
nickname_tbl = [dict(row.items()) for row in nickname_tbl]
nickname_tbl = [{str(k):str(v) for (k,v) in d.items()} for d in nickname_tbl]

pols_query = client.query("""
    select * from `{0}.{1}.{2}`""".format(project_id, dataset_id, pol_table_id))
pols_tbl = pols_query.result()
pols_tbl = [dict(row.items()) for row in pols_tbl]
pols_tbl = [{str(k):str(v) for (k,v) in d.items()} for d in pols_tbl]
# print(pols_tbl)

clean_pol = (p
             | 'Read from PubSub' >> beam.io.gcp.pubsub.ReadFromPubSub(topic = None,
                                     subscription = 'projects/{0}/subscriptions/{1}'
                                                  .format(project_id, subscription_name),
                                     with_attributes = True)
             | 'Isolate Attributes' >> beam.ParDo(IsolateAttrFn())
             # | 'Scrub First Name' >> beam.ParDo(ScrubFnameFn(), keep_suffix=True)  # make sure to capture nickname and suffixes
             # | 'Fix Nicknames' >> beam.ParDo(FixNicknameFn(), n_tbl=nickname_tbl, keep_nickname=True)
             # | 'Scrub Last Name' >> beam.ParDo(ScrubLnameFn())
             | 'Reformat Keys' >> beam.ParDo(ReformatKeysFn())) #suffixes are weird; double check formulas


class NewPolsOnlyFn(beam.DoFn):
    def process(self, element, pol_tbl):
        # determine if the politician is in the list
        matched_pols = []
        for i in pol_tbl:
            bool_list=[
                i['first_name']==element['first_name'],
                i['last_name']==element['last_name'],
                i['party']==element['party'],
                i['state']==element['state']
            ]
            if all(bool_list):
                matched_pols.append(i)
            else:
                pass
        # if so, remove
        if len(matched_pols)>0:
            pass
        else:
        # if not, return the element and move on
            logging.info('{0} is the output of the NewPolsOnlyFn.'.format(element))
            yield element

class AddDateFn(beam.DoFn):
    def process(self, element):
        print(element)
        date_today = dt.today()
        date_today = str(date_today)
        element['date'] = date_today
        element.pop('suffix')
        element.pop('nickname')
        logging.info('{0} is the output of the AddDateFn.'.format(element))
        yield element

new_pol = (clean_pol
           | 'Filter Existing Pols' >> beam.ParDo(NewPolsOnlyFn(), pol_tbl=pols_tbl)
           | 'Write Pol to BQ' >> beam.io.WriteToBigQuery(
            table=pol_spec,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        ))


new_senator = (clean_pol
               | 'Field Formatting' >> beam.ParDo(AddDateFn())
               | 'Write Sen to BQ' >> beam.io.WriteToBigQuery(
                    table=senate_spec,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        ))

p.run()