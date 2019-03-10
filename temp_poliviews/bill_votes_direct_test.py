### -------- Import all necessary modules -------- ###
# Get the logging set for debugging

import logging
import os

from google.cloud import pubsub
from apache_beam.io.gcp.internal.clients.bigquery import TableReference
from datetime import date
from datetime import datetime
import re
import unidecode
import nose

# all Apache modules needed
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.pubsub import PubsubMessage

# Steps to complete for the test
# 1) build out a Pub/Sub - Dataflow mock message that will repeatedly send the same example message upon execution of this code block
logging.basicConfig(level=logging.INFO)

project_id = 'politics-data-tracker-1'
topic_name = 'bill_votes'
subscription_name = 'test_bill_votes_sub'

dataset_id = 'poliviews'
table_id = 'all_votes'
bq_spec = '{0}:{1}.{2}'.format(project_id, dataset_id, table_id)

x_data = u'This is a representative in the House.'
x_data = x_data.encode('utf-8')

x_attributes = {
     'bill_id': 'S3600',
     'amdt_id': None,
     'pol_fn': 'Joe,',
     'pol_ln': 'Donnelly',
     'pol_party': 'D',
     'pol_state': 'IN',
     'vote_cast': 1,
     'vote_date': str(date(2018, 10, 23)),
     'house': 'HR',
     'state': 'US'
  }

x_attributes_str = {'bill_id': str(x_attributes['bill_id']),
               'amdt_id': str(x_attributes['amdt_id']),
               'pol_fn': str(x_attributes['pol_fn']),
               'pol_ln': str(x_attributes['pol_ln']),
               'pol_state': str(x_attributes['pol_state']),
               'pol_party': str(x_attributes['pol_party']),
               'vote_cast': str(x_attributes['vote_cast']),
               'vote_date': str(x_attributes['vote_date']),
               'house': str(x_attributes['house']),
               'state': str(x_attributes['state'])}

n_tbl_ex = [['Joe', 'Joseph'], ['Carol', 'Caroline']]

logging.info('Created test dataset.')

# Build publisher and publish to it
os.environ['GOOGLE_APPLICATION_CREDENTIALS']='C:\Users\cmatt\PycharmProjects\dataflow_scripts\gcp_credentials.txt'
publisher = pubsub.PublisherClient()

logging.info('Publisher client constructed.')

topic_path = publisher.topic_path(project_id, topic_name)
future = publisher.publish(topic_path, data=x_data,
                           bill_id = x_attributes_str['bill_id'],
                           amdt_id = x_attributes_str['amdt_id'],
                           pol_fn = x_attributes_str['pol_fn'],
                           pol_ln = x_attributes_str['pol_ln'],
                           pol_state = x_attributes_str['pol_state'],
                           pol_party = x_attributes_str['pol_party'],
                           vote_cast = x_attributes_str['vote_cast'],
                           vote_date = x_attributes_str['vote_date'],
                           house = x_attributes_str['house'],
                           state = x_attributes_str['state'])

logging.info('Published message ID {0}'.format(future.result()))

# # Build subscriber to check
subscription_name = 'test_bill_votes_sub'
subscriber = pubsub.SubscriberClient()
subscription_path = subscriber.subscription_path(
    project_id, subscription_name)

# 2) properly build out a TestPipeline that functions

options = PipelineOptions(streaming=True,
                          runner='DirectRunner')

class IsolateAttrFn(beam.DoFn):
    def process(self, element):
        e_type = type(element)
        logging.info('Element is now type {0}.'.format(e_type))
        attr_dict = element.attributes
        logging.info('Element is now type {0}.'.format(type(attr_dict)))
        attr_tuple = (attr_dict['bill_id'],
                      attr_dict['amdt_id'],
                      attr_dict['pol_fn'],
                      attr_dict['pol_ln'],
                      attr_dict['pol_state'],
                      attr_dict['pol_party'],
                      attr_dict['vote_cast'],
                      attr_dict['vote_date'],
                      attr_dict['house'],
                      attr_dict['state'])
        # logging.info('Element is now type {0}.'.format(type(attr_tuple)))
        print(attr_dict)
        yield attr_dict;

class FixTypesFn(beam.DoFn):
    def process(self, element):
        logging.info('The element is type {0} before FixTypesFn.'.format(type(element)))
        d = list(element)
        d = {k:str(v) for (k, v) in element.items()}
        print(d)
        d['vote_cast'] = int(float(d['vote_cast']))
        # d[7] = datetime.strptime(d[7], '%Y-%m-%d')
        # d = tuple(d)
        print(d)
        logging.info('The element is type {0} after FixTypesFn.'.format(type(d)))
        yield d

class CheckTypeFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        logging.info('This element is now type {0}'.format(type(element)))
        print(element)
        return tuple(element)

class ScrubFnameFn(beam.DoFn):  # Go through and remove everything from the first name I don't need
    def process(self, element):
        logging.info('This element is type {0} before ScrubFnameFn.'.format(type(element)))
        print(element)
        s = element['pol_fn']
        print(s)
        if len(re.findall(r'[A-Z]\.', s)) == 1:
            u = unidecode.unidecode(s)
            v = re.sub(r' [A-Z]\.', '', u)  # remove middle initials
            w = re.sub(r' \".*\"', '', v)  # remove nicknames
            match = re.search(r' (Sr.|Jr.|III|IV)', w)
            if match:
                # element['suffix'] = w[match.start + 1:match.end]
                x = re.sub(r' (Sr.|Jr.|III|IV)', '', w)
            else:
                x = w
            y = re.sub(r' \(.*\)', '', x)  # remove any parentheses
            z = re.sub(r'\,', '', y)  # remove stray commas
            a = z.strip()  # remove excess whitespace
            # new_element = list(element)
            element['pol_fn'] = a
            # new_element = tuple(new_element)
            logging.info('This element is type {0} after ScrubFnameFn.'.format(type(element)))
            print(element)
            yield element
        else:
            u = unidecode.unidecode(s)
            v = re.sub(r'\".*\"', '', u)  # remove nicknames
            match = re.search(r' (Sr.|Jr.|III|IV)', v)
            if match:
                # element['suffix'] = v[match.start + 1:match.end]
                w = re.sub(r' (Sr.|Jr.|III|IV)', '', v)
            else:
                w = v
            x = re.sub(r' \(.*\)', '', w)  # removes any parentheses
            y = re.sub(r'\,', '', x)  # remove stray commas
            z = y.strip()  # remove excess whitespace
            # NOTE: BUILD LIST COMPREHENSION HERE TO CREATE A NEW TUPLE
            # new_element = list(element)
            element['pol_fn'] = z
            # new_element = tuple(new_element)
            logging.info('This element is type {0} after ScrubFnameFn.'.format(type(element)))
            print(element)
            yield element

class ScrubLnameFn(beam.DoFn):  # Go through and remove everything from the last name I don't need
    def process(self, element):
        logging.info('This element is type {0} before ScrubLnameFn.'.format(type(element)))
        print(element)
        s = element['pol_ln']
        if len(re.findall(r'[A-Z]\.', s)) == 1:
            u = unidecode.unidecode(s)
            v = re.sub(r' [A-Z]\.', '', u)  # remove middle initials
            w = re.sub(r' \".*\"', '', v)  # remove nicknames
            match = re.search(r' (Sr.|Jr.|III|IV)', w)
            if match:
                # element['suffix'] = w[match.start + 1:match.end]
                x = re.sub(r' (Sr.|Jr.|III|IV)', '', w)
            else:
                # element['suffix'] = None
                x = w
            x = re.sub(r' (Sr.|Jr.|III|IV)', '', w)  # remove suffixes
            y = re.sub(r' \(.*\)', '', x)  # removes any parentheses
            z = re.sub(r',.*', '', y)  # remove anything after a comma
            a = re.sub(r'\,', '', z)  # remove stray commas
            b = a.strip()  # remove excess whitespace
            # new_element = list(element)
            element['pol_fn'] = b
            # element = tuple(new_element)
            logging.info('This element is type {0} after ScrubLnameFn.'.format(type(element)))
            yield element
        else:
            u = unidecode.unidecode(s)
            v = re.sub(r'\".*\"', '', u)  # remove nicknames
            match = re.search(r' (Sr.|Jr.|III|IV)', v)
            if match:
                # element['suffix'] = v[match.start + 1:match.end]
                w = re.sub(r' (Sr.|Jr.|III|IV)', '', v)
            else:
                # element['suffix'] = None
                w = v
            x = re.sub(r' \(.*\)', '', w)  # removes any parentheses
            y = re.sub(r',.*', '', x)  # remove anything after a comma
            z = re.sub(r'\,', '', y)  # remove stray commas
            a = z.strip()  # remove excess whitespace
            # new_element = list(element)
            element['pol_ln'] = a
            # new_element = tuple(new_element)
            logging.info('This element is type {0} after ScrubLnameFn.'.format(type(element)))
            yield element

class FixNicknameFn(beam.DoFn):
    def process(self, element, n_tbl):
        # new_element = list(element)
        fn = element['pol_fn']
        bool_list = [i[0]==fn for i in n_tbl]
        x = bool_list.index(True)
        if sum(bool_list)==1:
            fn=n_tbl[x][1]
        if sum(bool_list)>1:
            SyntaxError('There are multiple references of the nickname {0}.'.format(fn))
        else:
            pass
        element['pol_fn'] = fn
        # new_element = tuple(new_element)
        print(element)
        yield element

# p = beam.Pipeline(options = options)
p = TestPipeline(options=options)

logging.info('Created Dataflow pipeline.')

test_vote_info =(p
                 | beam.io.gcp.pubsub.ReadFromPubSub(topic = None,
                                     subscription = 'projects/{0}/subscriptions/{1}'.format(project_id, subscription_name),
                                     # id_label = 'messageID',
                                     with_attributes = True)
                 | 'Isolate Attributes' >> beam.ParDo(IsolateAttrFn())
                 | 'Fix Value Types' >> beam.ParDo(FixTypesFn())
                 | 'Scrub First Name' >> beam.ParDo(ScrubFnameFn())
                 | 'Fix Nicknames' >> beam.ParDo(FixNicknameFn(), n_tbl=n_tbl_ex)
                 | 'Scrub Last Name' >> beam.ParDo(ScrubLnameFn()))#

(test_vote_info | 'Write to BQ' >> beam.io.WriteToBigQuery(
    # beam.io.gcp.internal.clients.bigquery.TableReference(
    #     projectId=project_id,
    #     datasetId=dataset_id,
    #     tableId=table_id
    # ),
    table = bq_spec,
    write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND,
    create_disposition = beam.io.BigQueryDisposition.CREATE_NEVER
))
# test_vote_info | beam.io.WriteToText('C:/Users/cmatt/PycharmProjects/dataflow_scripts/runner_outputs/test_output.txt')
#
# (p
#  | 'Create' >> beam.Create([x_attributes])
#  | 'Write to BQ' >> beam.io.WriteToBigQuery(
#     # beam.io.gcp.internal.clients.bigquery.TableReference(
#     #     projectId=project_id,
#     #     datasetId=dataset_id,
#     #     tableId=table_id
#     # ),
#     table = bq_spec,
#     write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND,
#     create_disposition = beam.io.BigQueryDisposition.CREATE_NEVER
# ))

p.run()