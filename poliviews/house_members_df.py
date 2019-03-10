"""

This script is a pipeline built in order to record all of the politicians currently in the House. We will be receiving
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
import pandas as pd

# This module will closely track the progression of the PCollection through the pipeline.
# It has been set to report on the collection throughout its creation and augmentation.
logging.basicConfig(level=logging.INFO)

# Location of the PubSub topic and subscription we will be using to send/receive messages
project_id = 'politics-data-tracker-1'
topic_name = 'house_pols'
subscription_name = 'house_pols_df'

# Locations in BigQuery that this script uses to read/write.
dataset_id = 'poliviews'
house_table_id = 'house' # Location for writing all Rep info
pol_table_id = 'politicians' # Location for writing any new politicians
nickname_table_id = 'nicknames' # Location to read list of any nicknames in order to properly edit politician info.

# PubSub will only accept long strings as names of exact locations of BigQuery tables.
pol_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, pol_table_id)
house_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, house_table_id)

# Also included is the current name of the script
script_name = os.path.basename(sys.argv[0])

# This is a list of all attributes that will be present in either a good vote or error vote. This will be used
# to filter and/or order the attributes accordingly.
# At this time, this is necessary. Make sure to do this for all pipelines.
pol_attr_lst = ['first_name','last_name','party','state', 'suffix', 'nickname']
house_attr_lst = ['first_name', 'last_name', 'party', 'state', 'district', 'date']
error_attr_lst = ['error_msg', 'ptransform', 'script']
pol_full_lst = pol_attr_lst + error_attr_lst

# Here are the credentials needed of a service account in order to access GCP
# os.environ['GOOGLE_APPLICATION_CREDENTIALS']='gs://politics-data-tracker-1/dataflow/gcp_credentials.txt'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='C:\Users\cmatt\PycharmProjects\dataflow_scripts\poliviews\gcp_credentials.txt'

# Set all options needed to properly run the pipeline. This pipeline will run on Dataflow as a streaming pipeline.
options = PipelineOptions(streaming=True,
                          runner='DataflowRunner',
                          project=project_id,
                          temp_location='gs://{0}/tmp'.format(project_id),
                          staging_location='gs://{0}/staging'.format(project_id))
                          # extra_package=ptransform_file)

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

# The House elements pull states as full names instead of initials.
# This query as well as the following DoFn are meant to account for that and map the initals back to the proper states.
state_query = client.query("""
    select * from `{0}.{1}.{2}`""".format(project_id, dataset_id, 'state_map'))
state_tbl = pols_query.result()
state_tbl = [dict(row.items()) for row in pols_tbl]
state_tbl = [{str(k):str(v) for (k,v) in d.items()} for d in pols_tbl]

class StateMapFn(beam.DoFn):
    def process(self, element, tbl):
        try:
            tbl = pd.DataFrame(tbl)
            criteria = [tbl['state']==element['state']]
            tbl_matched = tbl[criteria]
            if len(tbl_matched.index) > 1:
                raise ValueError('There are multiple states in the database that match the given state.')
            elif len(tbl_matched.index) == 1:
                element['state'] = tbl_matched['initials'].values[0]
                yield element
            else:
                raise ValueError('There are no states in the database that match the given state.')
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element
        yield element

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
        | 'Map States' >> beam.ParDo(StateMapFn(), tbl=state_tbl)
        | 'Fix Nones' >> beam.ParDo(pt.FixNoneFn())
        | 'Tag Errors' >> beam.ParDo(pt.TagErrorsFn()).with_outputs('error_tag'))

error_pols = pol.error_tag
clean_pols = pol[None]

# A new Politician will only be published if thy are not already contained in the Politicians table. If they are new,
# they will be properly uploaded. If they are not new, then they will be ignored in this pipeline.
new_pol = (clean_pols
           | 'Filter Existing Pols' >> beam.ParDo(pt.NewPolsOnlyFn(), pol_tbl=pols_tbl)
           | 'Filter Keys' >> beam.ParDo(pt.FilterKeysFn(), attr_lst=pol_attr_lst)
           | 'Write Pol to BQ' >> beam.io.WriteToBigQuery(
            table=pol_spec,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        ))

# A new Rep line will be published every day in this pipeline. We add today's date to record their time in office
# and will upload a row signifying their time in office on that day.
new_rep = (clean_pols
               | 'Field Formatting' >> beam.ParDo(pt.AddDateFn())
               | 'Filter Keys' >> beam.ParDo(pt.FilterKeysFn(), attr_lst=house_attr_lst)
               | 'Write Rep to BQ' >> beam.io.WriteToBigQuery(
                    table=house_spec,
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

p.run()