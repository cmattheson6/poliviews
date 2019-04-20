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
from apache_beam.pvalue import AsList

# Date modules for date formatting
from datetime import date

# Miscellaneous modules
import re
import unidecode

# Import transforms file
import transforms.df_ptransforms as pt

# This module will closely track the progression of the PCollection through the pipeline.
# It has been set to report on the collection throughout its creation and augmentation.
logging.basicConfig(level=logging.INFO)

project_id = 'politics-data-tracker-1'
topic_name = 'error_handler'
subscription_name = 'error_handler'

# Location of all error files. Full file location requires adding the .csv extension from the PubSub attributes
error_file_dir = ''

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:\Users\cmatt\PycharmProjects\dataflow_scripts\poliviews\gcp_credentials.txt'

options = PipelineOptions(
    streaming=True,
    runner='DirectRunner',
    project=project_id,
    temp_location='gs://{0}/tmp'.format(project_id),
    staging_location='gs://{0}/staging'.format(project_id)
)

p = beam.Pipeline(options=options)
logging.info('DirectRunner Pipeline created.')

file_extension = ''
header = []

class FormatAttrFn(beam.DoFn):
    def process(self, element):
        global file_extension
        file_extension = element.pop('topic_name')
        global header
        header = element.keys()
        values = element.values()
        element=values
        yield element

### Look up 'Dynamic Destinations' for apache beam
### and 'File IO Write With Naming'

error_handled = (
    p
    | 'Read from PubSub' >> beam.io.ReadFromPubSub(
        topic=None,
        subscription='projects/{0}/subscriptions/{1}'.format(project_id, subscription_name),
        with_attributes=True
)
    | 'Isolate Attributes' >> beam.ParDo(pt.IsolateAttrFn())
    | 'Formatting' >> beam.ParDo(FormatAttrFn())
    | 'Write to CSV' >> beam.io.WriteToText(
        file_path_prefix='gs://{0}/error_files/{1}'.format(project_id, file_extension),
        file_name_suffix='.csv',
        append_trailing_newlines=True,
        shard_name_template='',
        header=header
))

p.run().wait_until_finish()