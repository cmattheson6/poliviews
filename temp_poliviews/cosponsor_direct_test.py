# -------- Import all necessary modules -------- #
# Get the logging set for debugging
import logging
import os
import sys

# Pull in all Pubsub, Dataflow, and BigQuery modules
from google.cloud import pubsub
from google.cloud import bigquery

# all Apache modules needed
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import transforms.df_ptransforms as pt
from apache_beam.testing.test_pipeline import TestPipeline

# THE BIGQUERY TABLE FOR THIS NEEDS TO BE FIXED
test_bill_info = {
    'bill_id':'S1000',
    'amdt_id': None,
    'cosponsor_fn':'Caroline M.',
    'cosponsor_ln':'Brantley IV',
    'cosponsor_party':'R',
    'cosponsor_state':'FL'
}


n_tbl_ex = [{'nickname': 'Joe', 'full_name': 'Joseph'},
            {'nickname': 'Carol', 'full_name': 'Caroline'}]

project_id = 'politics-data-tracker-1'
topic_name = 'bill_info'
subscription_name = 'bill_info_df'

dataset_id = 'poliviews'
cosponsors_table_id = 'bill_cosponsors'
nickname_table_id = 'nicknames'

bill_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, cosponsors_table_id)

x_data = u'This is a senator in the Senate.'
x_data = x_data.encode('utf-8')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:\Users\cmatt\PycharmProjects\dataflow_scripts\poliviews\gcp_credentials.txt'
logging.basicConfig(level=logging.INFO)

publisher = pubsub.PublisherClient()

logging.info('Publisher client constructed.')

topic_path = publisher.topic_path(project_id, topic_name)
future = publisher.publish(
    topic_path,
    data = x_data,
    bill_id = str(test_bill_info['bill_id']),
    amdt_id = str(test_bill_info['amdt_id']),
    cosponsor_fn = str(test_bill_info['cosponsor_fn']),
    cosponsor_ln = str(test_bill_info['cosponsor_ln']),
    cosponsor_party = str(test_bill_info['cosponsor_party']),
    cosponsor_state = str(test_bill_info['cosponsor_state']))

logging.info('Published message ID {0}.'.format(future.result()))

options = PipelineOptions(
    streaming=True,
    runner='DirectRunner',
    project=project_id,
    temp_location='gs://{0}/tmp'.format(project_id),
    staging_location='gs://{0}/staging'.format(project_id))

p = TestPipeline(options = options)

class NormalizeAttributesFn(beam.DoFn):
    def process(self, element):
        element['pol_fn']=element.pop('cosponsor_fn')
        element['pol_ln']=element.pop('cosponsor_ln')
        element['pol_party']=element.pop('cosponsor_party')
        element['pol_state']=element.pop('cosponsor_state')
        yield element

class RevertAttributesFn(beam.DoFn):
    def process(self, element):
        element['cosponsor_fn']=element.pop('pol_fn')
        element['cosponsor_ln']=element.pop('pol_ln')
        element['cosponsor_party']=element.pop('pol_party')
        element['cosponsor_state']=element.pop('pol_state')
        logging.info('Resulting element from RevertAttributesFn: {0}'.format(element))
        yield element

cleaned_bill = (
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
    | 'Write to BQ' >> beam.io.WriteToBigQuery(
        table=bill_spec,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
))

p.run()