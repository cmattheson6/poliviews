"""

This script is a pipeline built in order to record all of the bill cosponsors to any presented bills. We will receive
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
topic_name = 'cosponsors'
subscription_name = 'cosponsors_df'

# Locations in BigQuery that this script uses to read/write.
dataset_id = 'runner_pipelines'
cosponsors_table_id = 'bill_cosponsors'
nickname_table_id = 'nicknames'

# PubSub will only accept long strings as names of exact locations of BigQuery tables.
bill_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, cosponsors_table_id)
nickname_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, nickname_table_id)

# Also included is the current name of the script
script_name = os.path.basename(sys.argv[0])

# This is a list of all attributes that will be present in either a good vote or error vote. This will be used
# to filter and/or order the attributes accordingly.
# At this time, this is necessary. Make sure to do this for all pipelines.
attributes_lst = ['bill_id','amdt_id','first_name','last_name','party','state']
error_attr_lst = ['error_msg', 'ptransform', 'script']
full_lst = attributes_lst + error_attr_lst

# Set all options needed to properly run the pipeline. This pipeline will run on Dataflow as a streaming pipeline.
options = PipelineOptions(
    streaming=False,
    runner='DirectRunner',
    project=project_id,
    temp_location='gs://{0}/tmp'.format(project_id),
    staging_location='gs://{0}/staging'.format(project_id))

# This builds the Beam pipeline in order to run Dataflow
p = beam.Pipeline(options = options)
logging.info('Created Dataflow pipeline.')

class NormalizeAttributesFn(beam.DoFn):
    def process(self, element):
        try:
            element['first_name']=element.pop('cosponsor_fn')
            element['last_name']=element.pop('cosponsor_ln')
            element['party']=element.pop('cosponsor_party')
            element['state']=element.pop('cosponsor_state')
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
            element['cosponsor_fn']=element.pop('first_name')
            element['cosponsor_ln']=element.pop('last_name')
            element['cosponsor_party']=element.pop('party')
            element['cosponsor_state']=element.pop('state')
            yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element

# This will pull in all of the recorded nicknames to compare to the incoming PubSubMessages. This is needed to filter
# and normalize the data.
client = bigquery.Client()
query_job = client.query("""
    select * from `{0}.{1}.{2}`""".format(project_id, dataset_id, nickname_table_id))
nickname_tbl = query_job.result()
nickname_tbl = [dict(row.items()) for row in nickname_tbl]
nickname_tbl = [{str(k):str(v) for (k,v) in d.items()} for d in nickname_tbl]

class SplitFn(beam.DoFn):
    def process(self, element):
        for i in element:
            index, \
            bill_id, \
            amdt_id, \
            cosponsor_fn, \
            cosponsor_ln, \
            cosponsor_party, \
            cosponsor_state = element.split(',')

            d = {
                'bill_id': bill_id,
                'amdt_id': amdt_id,
                'cosponsor_fn': cosponsor_fn,
                'cosponsor_ln': cosponsor_ln,
                'cosponsor_party': cosponsor_party,
                'cosponsor_state': cosponsor_state
            }
            yield d

cosponsor = (
    p
    | 'Read from CSV' >> beam.io.ReadFromText('{0}/tmp/cosponsors/*.csv'.format(os.path.expanduser('~')),
                                              skip_header_lines=1)
    | 'Split Values' >> beam.ParDo(SplitFn())
    | 'Normalize Attributes' >> beam.ParDo(NormalizeAttributesFn())
    | 'Scrub First Name' >> beam.ParDo(pt.ScrubFnameFn(), keep_suffix=False)
    | 'Fix Nicknames' >> beam.ParDo(pt.FixNicknameFn(), n_tbl = nickname_tbl, keep_nickname=False)
    | 'Scrub Last Name' >> beam.ParDo(pt.ScrubLnameFn())
    # | 'Revert Attributes' >> beam.ParDo(RevertAttributesFn())
    | 'Fix Nones' >> beam.ParDo(pt.FixNoneFn())
    | 'Tag Errors' >> beam.ParDo(pt.TagErrorsFn()).with_outputs('error_tag'))

# Separates the clean and error votes for proper processing.
error_cosponsors = cosponsor.error_tag
clean_cosponsors = cosponsor[None]

(clean_cosponsors
    | 'Filter Keys' >> beam.ParDo(pt.FilterKeysFn(), attr_lst=attributes_lst)
    | 'Write to BQ' >> beam.io.WriteToBigQuery(
        table=bill_spec,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
))

(error_cosponsors
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