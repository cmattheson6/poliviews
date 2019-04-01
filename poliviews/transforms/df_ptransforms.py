"""

This module contains all of the Transforms that are not unique to a specific pipeline. This module is referenced in the
setup.py file for this project and will be downloaded by any worker node that runs a Dataflow pipeline.

There are a few different types of DoFns needed for these pipelines:
1) Extracting the attributes from the Pub/Sub message.
2) Reformatting the element to normalize for upload. This can be the politician's name
or the types of an element's items.
3) Error handling and key handling fxns to keep the pipeline running smoothly.

"""

# -------- Import all necessary modules -------- #
# Get the logging set for debugging
import logging
import os
import sys

# all Apache modules needed
import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam import pvalue

# Miscellaneous modules
import re
import pandas as pd

error_tag = 'error_tag'

"""
1) Add error tags to all of these
2) Add Try and Except to all of these
3) Rearrange and add clear comments
4) Make sure all attributes line up b/t Scrapy and Dataflow scripts.
"""

# This DoFn pulls out the attributes of a PubSubMessage into a dictionary.
class IsolateAttrFn(beam.DoFn):
    def process(self, element):
        try:
            attr_dict = element.attributes
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            yield attr_dict
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element

# Restructure element into a PubSubMessage if anything needs to be sent back to Pub/Sub from Dataflow.
class BuildPSMFn(beam.DoFn):
    def process(self, element, s='No message provided.'):
        try:
            import apache_beam as beam
            element = {k:str(v) for (k,v) in element}
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            element = beam.io.gcp.pubsub.PubsubMessage(
                data=s,
                attributes=element
            )
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element

# All data points come in as unicode. This converts all non-string data points into the proper data type.
# In this case, it is just the vote that needs to be converted to an integer.
class FixTypesFn(beam.DoFn):
    def process(self, element, int_lst):
        try:
            d = {str(k):str(v) for (k, v) in element.items()}
            d = {k:(int(v) if k in int_lst else v) for (k,v) in element.items()}
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            yield d
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element

### Because all values are imported as strings, make sure to resolve any None string as an actual None value
class FixNoneFn(beam.DoFn):
    def process(self, element):
        try:
            element = {k:(None if v=='None' else v) for (k,v) in element.items()}
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element


# To properly tag elements in a pipeline, it needs to be done at the end of the pipeline.
# Throughout the pipeline, an error is handled by adding an 'error_tag' key into the element.
# This DoFn should be used at the end of the initial formatting pipeline.
# Then if there was an error, all of the error info has been captured and can be sent to a csv
# for later re-processing. This allows the pipeline to continue running without error and still
# capture the error-causing element.
class TagErrorsFn(beam.DoFn):
    def process(self, element):
        if 'error_tag' in element.keys():
            element.pop('error_tag')
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            yield pvalue.TaggedOutput(error_tag, element)
        else:
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            yield element

# In order to map properly, we need to reduce all names to a uniform unit.
# The next four fxns will filter, alter, and output as close to that as possible
# and make it a 'pure' representation of a politician's name.

# Parses out a suffix if there is one in a name string. Will return both the new name and the suffix.
def check_suffix(s):
    match = re.search(r' (Sr.|Jr.|III|IV)', s)
    if match:
        suffix = s[match.start():match.end()].strip()
        s = re.sub(r' (Sr.|Jr.|III|IV)', '', s)
    else:
        suffix = None
    return s, suffix

# Filters the name appropriately to remove any extraneous characters.
def scrub_name(s):
    if len(re.findall(r'[A-Z]\.', s)) == 1:
        s = re.sub(r' [A-Z]\.', '', s)  # remove middle initials
    s = re.sub(r' \".*\"', '', s)  # remove nicknames
    s, suffix = check_suffix(s)
    s = re.sub(r' \(.*\)', '', s)  # remove any parentheses
    s = re.sub(r'\,', '', s)  # remove stray commas
    s = s.strip()  # remove excess whitespace
    return s, suffix

# Normalizes the first name of an element. Will return both the name and suffix if the pipeline calls for it.
class ScrubFnameFn(beam.DoFn):
    def process(self, element, keep_suffix=False):
        try:
            s = element['first_name']
            s, suffix = scrub_name(s)
            element['first_name'] = s
            if keep_suffix==True:
                element['suffix'] = suffix
            else:
                pass
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element

# Same purpose as above but for last names.
class ScrubLnameFn(beam.DoFn):
    def process(self, element, keep_suffix=False):
        try:
            s = element['last_name']
            s, suffix = scrub_name(s)
            element['last_name'] = s
            if keep_suffix==True:
                if element['suffix']==None:
                    element['suffix'] = suffix
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element

# If a politician's name is referenced multiple ways in different sources, this will
# simplify the two data sources to a uniform descriptor. It references a BigQuery table
# that we keep updated based on any discrepancies we run into.
class FixNicknameFn(beam.DoFn):
    def process(self, element, n_tbl, keep_nickname=False, nickname=None):
        try:
            fn = element['first_name']
            n_list=[]
            for i in n_tbl:
                if i['nickname']==fn:
                    n_list.append(i)
                else:
                    pass
            if len(n_list)==1:
                nickname=fn
                element['first_name']=n_list[0]['full_name']
            if len(n_list)>1:
                SyntaxError('There are multiple references of the name {0} for element {1}.'.format(fn, element))
            else:
                pass
            if keep_nickname == True:
                element['nickname'] = nickname
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element

# The House votes table does not pull in first names. This DoFn is to properly map and add in the first name
# of the correct politician.
class FixHouseFirstNameFn(beam.DoFn):
    def process(self, element, tbl):
        try:
            if element['first_name'] == None:
                tbl = pd.DataFrame(tbl)
                criteria = [
                    (tbl['last_name'] == element['last_name'])
                and (tbl['party'] == element['party'])
                and (tbl['state'] == element['state'])
                and (tbl['vote_date'] == element['vote_date'])
                ]
                tbl_matched = tbl[criteria]
                if len(tbl_matched.index) > 1:
                    raise ValueError('There are multiple politicians with that name active on that day.')
                elif len(tbl_matched.index) == 1:
                    element['first_name'] = tbl_matched['first_name'].values[0]
                    yield element
                else:
                    raise ValueError('There are no politicians with that name active on that day.')
            else:
                yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element


# This DoFn ensures that only an approved list of keys will be imported into BigQuery.
class FilterKeysFn(beam.DoFn):
    def process(self, element, attr_lst):
        {k:v for (k,v) in element.items() if k in attr_lst}
        if all([k in attr_lst for k in element.keys()]):
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            yield element
        else:
            missing_keys = [k for k in element.keys() if k not in attr_lst]
            raise IndexError('{0} not in attr_lst'.format(missing_keys))

# For the House and Senate table, today's date is appended to the element before uploading.
class AddDateFn(beam.DoFn):
    def process(self, element):
        try:
            from datetime import date
            element['date'] = date.today()
            yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element

# This DoFn's purpose is to check whether or not a politician has previously been stored in the database.
# If so, this part of the pipeline will filter it out and move on to the next element.
class NewPolsOnlyFn(beam.DoFn):
    def process(self, element, pol_tbl):
        try:
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
                logging.info('{0}: {1} is not a new element.'.format(self.__class__.__name__, element))
                pass
            else:
                # if not, return the element and move on
                logging.info('{0}: {1} is a new element and will be uploaded.'.format(self.__class__.__name__, element))
                yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element

# This will turn all attributes of an element into a string.
class MakeAllStringsFn(beam.DoFn):
    def process(self, element):
        try:
            element = {k:str(v) for (k,v) in element.items()}
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element

# This will join all strings into one for processing into a CSV error file.
# It also order the items in an element that are not a part of the approved list of values.
class BuildCSVRowFn(beam.DoFn):
    def process(self, element, lst):
        try:
            values = [element[i] if i in lst else '' for i in lst]
            element= ','.join(values)
            logging.info('{0}: {1}'.format(self.__class__.__name__, element))
            yield element
        except Exception as e:
            element['error_msg'] = e
            element['ptransform'] = self.__class__.__name__
            element['error_tag'] = True
            element['script'] = os.path.basename(sys.argv[0])
            logging.info('Error received at {0}: {1}'.format(self.__class__.__name__, element))
            yield element
