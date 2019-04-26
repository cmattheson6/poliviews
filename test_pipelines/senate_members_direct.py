"""

This script is a pipeline built in order to record all of the politicians currently in the Senate. We will be receiving
these votes from a Scrapy pipeline that has sent this data to PubSub. This pipeline receives the data from PubSub,
formats and filters the data, and then sends along the clean data to BQ for storage.

"""

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

# Import transforms file
import transforms.df_ptransforms as pt

# This module will closely track the progression of the PCollection through the pipeline.
# It has been set to report on the collection throughout its creation and augmentation.
logging.basicConfig(level=logging.INFO)

# Location of the PubSub topic and subscription we will be using to send/receive messages
project_id = 'politics-data-tracker-1'
topic_name = 'senate_pols'
subscription_name = 'senate_pols_df'

# Locations in BigQuery that this script uses to read/write.
dataset_id = 'runner_pipelines'
senate_table_id = 'senate' # Location for writing all senator info
pol_table_id = 'politicians' # Location for writing any new politicians
nickname_table_id = 'nicknames' # Location to read list of any nicknames in order to properly edit politician info.

# PubSub will only accept long strings as names of exact locations of BigQuery tables.
pol_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, pol_table_id)
senate_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, senate_table_id)

# Also included is the current name of the script
script_name = os.path.basename(sys.argv[0])

# This is a list of all attributes that will be present in either a good vote or error vote. This will be used
# to filter and/or order the attributes accordingly.
# At this time, this is necessary. Make sure to do this for all pipelines.
pol_attr_lst = ['first_name','last_name','party','state', 'suffix', 'nickname']
senate_attr_lst = ['first_name', 'last_name', 'party', 'state', 'date']
error_attr_lst = ['error_msg', 'ptransform', 'script']
pol_full_lst = pol_attr_lst + error_attr_lst

# Here are the credentials needed of a service account in order to access GCP
# os.environ['GOOGLE_APPLICATION_CREDENTIALS']='gs://politics-data-tracker-1/dataflow/gcp_credentials.txt'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='C:\Users\cmatt\PycharmProjects\dataflow_scripts\poliviews\gcp_credentials.txt'

### START DELETE HERE

publisher = pubsub.PublisherClient()

test_senator_old = {
    'first_name': 'Cameron',
    'last_name': 'Mattheson',
    'party': 'R',
    'state': 'MA'
}
test_senator_new = {
    'first_name': 'Carol',
    'last_name': 'Brantley IV',
    'party': 'R',
    'state': 'FL'
}


n_tbl_ex = [{'nickname': 'Joe', 'full_name': 'Joseph'},
            {'nickname': 'Carol', 'full_name': 'Caroline'}]
pol_tbl_ex = [{'first_name': 'Cameron',
               'last_name': 'Mattheson',
               'party': 'R',
               'state': 'MA',
               'suffix': None,
               'nickname': None}]
# date_today = str(date.today())

logging.info('Created test data set.')


x_data = u'This is a senator in the Senate.'
x_data = x_data.encode('utf-8')


logging.info('Publisher client constructed.')

topic_path = publisher.topic_path(project_id, topic_name)
future = publisher.publish(topic_path,
                           data=x_data,
                           pol_fn=test_senator_old['first_name'],
                           pol_ln=test_senator_old['last_name'],
                           pol_party=test_senator_old['party'],
                           pol_state=test_senator_old['state'])

logging.info('Published message ID {0}'.format(future.result()))

future = publisher.publish(topic_path,
                           data=x_data,
                           pol_fn=test_senator_new['first_name'],
                           pol_ln=test_senator_new['last_name'],
                           pol_party=test_senator_new['party'],
                           pol_state=test_senator_new['state'])

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

### DELETE STOPS HERE

# Set all options needed to properly run the pipeline. This pipeline will run on Dataflow as a streaming pipeline.
options = PipelineOptions(streaming=True,
                          runner='DataflowRunner',
                          project=project_id,
                          temp_location='gs://{0}/tmp'.format(project_id),
                          staging_location='gs://{0}/staging'.format(project_id))

# This builds the Beam pipeline in order to run Dataflow
p = beam.Pipeline(options=options)
logging.info('Created Dataflow pipeline.')

# This will pull in all of the recorded nicknames to compare to the incoming PubSubMessages. This is needed to filter
# and normalize the data.
client = bigquery.Client()
nickname_query = client.query("""
    select * from `{0}.{1}.{2}`""".format(project_id, dataset_id, nickname_table_id))
nickname_tbl = nickname_query.result()
nickname_tbl = [dict(row.items()) for row in nickname_tbl]
nickname_tbl = [{str(k):str(v) for (k,v) in d.items()} for d in nickname_tbl]

# This will pull in all of the recorded politicians to compare to the incoming PubSubMessages. This is needed to filter
# out any politicians that have already been recorded and only append new politicians.
pols_query = client.query("""
    select * from `{0}.{1}.{2}`""".format(project_id, dataset_id, pol_table_id))
pols_tbl = pols_query.result()
pols_tbl = [dict(row.items()) for row in pols_tbl]
pols_tbl = [{str(k):str(v) for (k,v) in d.items()} for d in pols_tbl]

# Runs the main part of the pipeline. Errors will be tagged, clean politicians will continue on to BQ.
pol = (
        p
        | 'Read from PubSub' >> beam.io.gcp.pubsub.ReadFromPubSub(
            topic = None,
            subscription = 'projects/{0}/subscriptions/{1}'
                      .format(project_id, subscription_name),
            with_attributes = True)
        | 'Isolate Attributes' >> beam.ParDo(pt.IsolateAttrFn())
        | 'Scrub First Name' >> beam.ParDo(pt.ScrubFnameFn(), keep_suffix=True)
        | 'Fix Nicknames' >> beam.ParDo(pt.FixNicknameFn(), n_tbl=nickname_tbl, keep_nickname=True)
        | 'Scrub Last Name' >> beam.ParDo(pt.ScrubLnameFn())
        | 'Fix Nones' >> beam.ParDo(pt.FixNoneFn())
        | 'Tag Errors' >> beam.ParDo(pt.TagErrorsFn()).with_outputs('error_tag'))

error_pols = pol.error_tag
clean_pols = pol[None]

# A new Politician will only be published if thy are not already contained in the Politicians table. If they are new,
# they will be properly uploaded. If they are not new, then they will be ignored in this pipeline.
new_pol = (
        clean_pols
        | 'Filter Existing Pols' >> beam.ParDo(pt.NewPolsOnlyFn(), pol_tbl=pols_tbl)
        | 'Filter Pol Keys' >> beam.ParDo(pt.FilterKeysFn(), attr_lst=pol_attr_lst)
        | 'Write Pol to BQ' >> beam.io.WriteToBigQuery(
            table=pol_spec,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        ))

# A new Senator line will be published every day in this pipeline. We add today's date to record their time in office
# and will upload a row signifying their time in office on that day.
new_senator = (
        clean_pols
        | 'Add Date' >> beam.ParDo(pt.AddDateFn())
        | 'Filter Sen Keys' >> beam.ParDo(pt.FilterKeysFn(), attr_lst=senate_attr_lst)
        | 'Write Sen to BQ' >> beam.io.WriteToBigQuery(
            table=senate_spec,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        ))

# This pipeline will take all of the error votes, clean them, properly orders and formats them,
# And outputs them into a friendly CSV format.
(error_pols
    | 'Make All Strings' >> beam.ParDo(pt.MakeAllStringsFn())
    | 'CSV Formatting' >> beam.ParDo(pt.BuildCSVRowFn(), lst=pol_full_lst)
    | 'Write to CSV' >> beam.io.WriteToText(
        # 'C:/Users/cmatt/Documents/politics-data-tracker-1/error_files',
        'gs://{0}/error_files/{1}'.format(project_id, script_name),
        file_name_suffix='.csv',
        append_trailing_newlines=True,
        num_shards=0,
        header=','.join(pol_full_lst)
    ))

# Run the pipeline
p.run()