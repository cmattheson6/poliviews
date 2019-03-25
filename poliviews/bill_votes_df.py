"""

This script is a pipeline built in order to record all of the bill votes from the House and Senate. We will be receiving
these votes from a Scrapy pipeline that has sent this data to PubSub. This pipeline receives the data from PubSub,
formats and filters the data, and then sends along the clean data to BQ for storage.

"""

### NEED TO FIX THIS PIPELINE TO HANDLE HOUSE MEMBERS; CURRENTLY DON"T HAVE FIRST NAME

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

# Date modules for date formatting
from datetime import date

# Miscellaneous modules
import re
import unidecode
from datetime import date, timedelta

# Import transforms file
import transforms.df_ptransforms as pt

# This module will closely track the progression of the PCollection through the pipeline.
# It has been set to report on the collection throughout its creation and augmentation.
logging.basicConfig(level=logging.INFO)

# Location of the PubSub topic and subscription we will be using to send/receive messages
project_id = 'politics-data-tracker-1'
topic_name = 'bill_votes'
subscription_name = 'test_bill_votes_sub'

# Locations in BigQuery that this script uses to read/write.
dataset_id = 'poliviews'
dest_table_id = 'all_votes' # Location for writing any voting information
nickname_table_id = 'nicknames' # Location to read list of any nicknames in order to properly edit politician info.

# PubSub will only accept long strings as names of exact locations of BigQuery tables.
bq_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, dest_table_id)
nickname_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, nickname_table_id)

# Also included is the current name of the script
script_name = os.path.basename(sys.argv[0])

# This is a list of all attributes that will be present in either a good vote or error vote. This will be used
# to filter and/or order the attributes accordingly.
# At this time, this is necessary. Make sure to do this for all pipelines.
attributes_lst = ['bill_id','amdt_id','first_name','last_name','party','state','vote_cast','vote_date','chamber']
error_attr_lst = ['error_msg', 'ptransform', 'script']
full_lst = attributes_lst + error_attr_lst

# Set date for yesterday's bills that are published
date_yesterday = date.today() - timedelta(days=1)
this_year = date_yesterday.year

# Here are the credentials needed of a service account in order to access GCP
# os.environ['GOOGLE_APPLICATION_CREDENTIALS']='gs://politics-data-tracker-1/dataflow/gcp_credentials.txt'
# os.environ['GOOGLE_APPLICATION_CREDENTIALS']='C:\Users\cmatt\PycharmProjects\dataflow_scripts\poliviews\gcp_credentials.txt'

# Set all options needed to properly run the pipeline. This pipeline will run on Dataflow as a streaming pipeline.
options = PipelineOptions(streaming=True,
                          runner='DataflowRunner',
                          project=project_id,
                          temp_location='gs://{0}/tmp'.format(project_id),
                          staging_location='gs://{0}/staging'.format(project_id))

# This builds the Beam pipeline in order to run Dataflow
p = beam.Pipeline(options = options)
logging.info('Created Dataflow pipeline.')

# This will pull in all of the recorded nicknames to compare to the incoming PubSubMessages. This is needed to filter
# and normalize the data.
client = bigquery.Client()
nickname_query = client.query("""
    select * from `{0}.{1}.{2}`""".format(project_id, dataset_id, nickname_table_id))
nickname_tbl = nickname_query.result()
nickname_tbl = [dict(row.items()) for row in nickname_tbl]
nickname_tbl = [{str(k):str(v) for (k,v) in d.items()} for d in nickname_tbl]

house_query = client.query("""
    select * from `{0}.{1}.{2}`""".format(project_id, dataset_id, 'house', date_yesterday))
house_tbl = house_query.result()
house_tbl = [dict(row.items()) for row in house_tbl]
house_tbl = [{str(k):str(v) for (k,v) in d.items()} for d in house_tbl]


# Runs the main part of the pipeline. Errors will be tagged, good votes will continue on to BQ.
vote = (
    p
    | beam.io.gcp.pubsub.ReadFromPubSub(
        topic = None,
        subscription = 'projects/{0}/subscriptions/{1}'
            .format(project_id, subscription_name),
        with_attributes = True)
    | 'Isolate Attributes' >> beam.ParDo(pt.IsolateAttrFn())
    | 'Fix Value Types' >> beam.ParDo(pt.FixTypesFn(), int_lst=['vote_cast'])
    | 'Fix House First Names' >> beam.ParDo(pt.FixHouseFirstNameFn(), tbl=house_tbl)
    | 'Scrub First Name' >> beam.ParDo(pt.ScrubFnameFn())
    | 'Fix Nicknames' >> beam.ParDo(pt.FixNicknameFn(), n_tbl=nickname_tbl)
    | 'Scrub Last Name' >> beam.ParDo(pt.ScrubLnameFn())
    | 'Fix Nones' >> beam.ParDo(pt.FixNoneFn())
    | 'Tag Errors' >> beam.ParDo(pt.TagErrorsFn()).with_outputs('error_tag'))

# Separates the clean and error votes for proper processing.
error_votes = vote.error_tag
clean_votes = vote[None]

# The cleaned elements will be sent to the proper BQ table for storage.
(clean_votes
    | 'Filter Keys' >> beam.ParDo(pt.FilterKeysFn(), attr_lst=attributes_lst)
    | 'Write to BQ' >> beam.io.WriteToBigQuery(
        table = bq_spec,
        write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition = beam.io.BigQueryDisposition.CREATE_NEVER
))

# This pipeline will take all of the error votes, clean them, properly orders and formats them,
# And outputs them into a friendly CSV format.
(error_votes
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