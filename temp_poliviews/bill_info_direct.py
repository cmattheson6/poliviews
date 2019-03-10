"""

This script is a pipeline built in order to track all bills written by and presented to Congress. We will be receiving
these votes from a Scrapy pipeline that has sent this data to PubSub. This pipeline recieves the data from PubSub,
formats and filters the data, and then sends along the clean data to BQ for storage.

"""

# -------- Import all necessary modules -------- #
# Get the logging set for debugging
import logging
import os
import sys

# Pull in all Pubsub, Dataflow, and BigQuery modules
from google.cloud import pubsub

# all Apache modules needed
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub

# Import transforms file
import transforms.df_ptransforms as pt

# This module will closely track the progression of the PCollection through the pipeline.
# It has been set to report on the collection throughout its creation and augmentation.
logging.basicConfig(level=logging.INFO)

# Location of the PubSub topic and subscription we will be using to send/receive messages
project_id = 'politics-data-tracker-1'
topic_name = 'bill_info'
subscription_name = 'bill_info_df'

# Locations in BigQuery that this script uses to read/write.
dataset_id = 'poliviews'
bill_table_id = 'bill_info' # Location for writing any bill info
nickname_table_id = 'nicknames' # Location to read list of any nicknames in order to properly edit politician info.

# PubSub will only accept long strings as names of exact locations of BigQuery tables.
bill_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, bill_table_id)

# Also included is the current name of the script
script_name = os.path.basename(sys.argv[0])


# This is a list of all attributes that will be present in either a good vote or error vote. This will be used
# to filter and/or order the attributes accordingly.
# At this time, this is necessary. Make sure to do this for all pipelines.
attributes_lst = [
    'bill_id',
    'amdt_id',
    'bill_title',
    'bill_summary',
    'sponsor_fn',
    'sponsor_ln',
    'sponsor_party',
    'sponsor_state',
    'bill_url']
error_attr_lst = ['error_msg', 'ptransform', 'script']
full_lst = attributes_lst + error_attr_lst

# Here are the credentials needed of a service account in order to access GCP
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:\Users\cmatt\PycharmProjects\dataflow_scripts\poliviews\gcp_credentials.txt'

### DELETE THIS
test_bill_info = {
    'bill_id': 'S1000',
    'amdt_id': None,
    'bill_title': 'Brantley Initialization Act of 2019',
    'bill_summary': 'Carol is opening a bakery.',
    'sponsor_fn': 'Caroline M.',
    'sponsor_ln': 'Brantley IV',
    'sponsor_party': 'R',
    'sponsor_state': 'FL',
    'bill_url': 'https://www.carolsbakery.com'
}

# Removed first name to test if it handles errors correctly.
test_bill_info_w_error = {
    'bill_id': 'S1000',
    'amdt_id': None,
    'bill_title': 'Brantley Initialization Act of 2019',
    'bill_summary': 'Carol is opening a bakery.',
    'sponsor_ln': 'Brantley IV',
    'sponsor_party': 'R',
    'sponsor_state': 'FL',
    'bill_url': 'https://www.carolsbakery.com'
}


n_tbl_ex = [{'nickname': 'Joe', 'full_name': 'Joseph'},
            {'nickname': 'Carol', 'full_name': 'Caroline'}]


x_data = u'This is a senator in the Senate.'
x_data = x_data.encode('utf-8')

publisher = pubsub.PublisherClient()

logging.info('Publisher client constructed.')

topic_path = publisher.topic_path(project_id, topic_name)
future = publisher.publish(
    topic_path,
    data = x_data,
    bill_id = str(test_bill_info['bill_id']),
    amdt_id = str(test_bill_info['amdt_id']),
    bill_title = str(test_bill_info['bill_title']),
    bill_summary = str(test_bill_info['bill_summary']),
    sponsor_fn = str(test_bill_info['sponsor_fn']),
    sponsor_ln = str(test_bill_info['sponsor_ln']),
    sponsor_party = str(test_bill_info['sponsor_party']),
    sponsor_state = str(test_bill_info['sponsor_state']),
    bill_url = str(test_bill_info['bill_url'])
)

logging.info('Published message ID {0}.'.format(future.result()))

future = publisher.publish(
    topic_path,
    data = x_data,
    bill_id = str(test_bill_info_w_error['bill_id']),
    amdt_id = str(test_bill_info_w_error['amdt_id']),
    bill_title = str(test_bill_info_w_error['bill_title']),
    bill_summary = str(test_bill_info_w_error['bill_summary']),
    sponsor_ln = str(test_bill_info_w_error['sponsor_ln']),
    sponsor_party = str(test_bill_info_w_error['sponsor_party']),
    sponsor_state = str(test_bill_info_w_error['sponsor_state']),
    bill_url = str(test_bill_info_w_error['bill_url'])
)

logging.info('Published message ID {0}.'.format(future.result()))

### STOP DELETING HERE

# Set all options needed to properly run the pipeline. This pipeline will run on Dataflow as a streaming pipeline.
options = PipelineOptions(
    streaming=True,
    runner='DataflowRunner',
    project=project_id,
    temp_location='gs://{0}/tmp'.format(project_id),
    staging_location='gs://{0}/staging'.format(project_id))

# This builds the Beam pipeline in order to run Dataflow
p = beam.Pipeline(options = options)
logging.info('Created Dataflow pipeline.')

class NormalizeAttributesFn(beam.DoFn):
    def process(self, element):
        try:
            element['first_name']=element.pop('sponsor_fn')
            element['last_name']=element.pop('sponsor_ln')
            element['party']=element.pop('sponsor_party')
            element['state']=element.pop('sponsor_state')
            yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element

class RevertAttributesFn(beam.DoFn):
    def process(self, element):
        try:
            element['sponsor_fn']=element.pop('first_name')
            element['sponsor_ln']=element.pop('last_name')
            element['sponsor_party']=element.pop('party')
            element['sponsor_state']=element.pop('state')
            yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element

# Runs the main part of the pipeline. Errors will be tagged, good votes will continue on to BQ.
bill = (
    p
    | 'Read from PubSub' >> beam.io.gcp.pubsub.ReadFromPubSub(
        topic=None,
        subscription='projects/{0}/subscriptions/{1}'.format(project_id, subscription_name),
        with_attributes=True)
    | 'Isolate Attributes' >> beam.ParDo(pt.IsolateAttrFn())
    | 'Normalize Attributes' >> beam.ParDo(NormalizeAttributesFn())
    | 'Scrub First Name' >> beam.ParDo(pt.ScrubFnameFn(), keep_suffix=False)
    | 'Fix Nicknames' >> beam.ParDo(pt.FixNicknameFn(), n_tbl = n_tbl_ex, keep_nickname=False)
    | 'Scrub Last Name' >> beam.ParDo(pt.ScrubLnameFn())
    | 'Revert Attributes' >> beam.ParDo(RevertAttributesFn())
    | 'Fix Nones' >> beam.ParDo(pt.FixNoneFn())
    | 'Tag Errors' >> beam.ParDo(pt.TagErrorsFn()).with_outputs('error_tag'))

# Separates the clean and error votes for proper processing.
error_bills = bill.error_tag
clean_bills = bill[None]

# The cleaned elements will be sent to the proper BQ table for storage.
(clean_bills
    | 'Filter Keys' >> beam.ParDo(pt.FilterKeysFn(), attr_lst=attributes_lst)
    | 'Write to BQ' >> beam.io.WriteToBigQuery(
    table=bill_spec,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
))

# This pipeline will take all of the error votes, clean them, properly orders and formats them,
# And outputs them into a friendly CSV format.
(error_bills
    | 'Make All Strings' >> beam.ParDo(pt.MakeAllStringsFn())
    | 'CSV Formatting' >> beam.ParDo(pt.BuildCSVRowFn(), lst=full_lst)
    | 'Write to CSV' >> beam.io.WriteToText(
        # 'C:/Users/cmatt/Documents/politics-data-tracker-1/error_files',
        'gs://{0}/error_files/{1}'.format(project_id, script_name),
        file_name_suffix='.csv',
        append_trailing_newlines=True,
        num_shards=0,
        header=','.join(full_lst)
    ))

# Run the pipeline
p.run()