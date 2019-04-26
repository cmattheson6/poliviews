'''
Created on Jul 10, 2018

@author: cmatt
'''
#list out all imported modules here

import psycopg2
import unidecode
import re
import pandas as pd

#First, open up the database connection

def scrub_name(s):
    if len(re.findall(r'[A-Z]\.', s)) == 1:
        u = unidecode.unidecode(s)
        v = re.sub(r' \".*\"', '', u) #remove nicknames
        w = re.sub(r' (Sr.|Jr.|III|IV)', '', v) #remove suffixes
        x = re.sub(r'\,', '', w) #remove stray commas
        y = re.sub(r' [A-Z]\.', '', x) #remove middle initials
        z = y.strip() #remove excess whitespace
        return z;
    else:
        u = unidecode.unidecode(s)
        v = re.sub(r'\".*\"', '', u) #remove nicknames
        w = re.sub(r' (Sr.|Jr.|III|IV)', '', v) #remove suffixes
        x = re.sub(r'\,', '', w) #remove stray commas
        y = x.strip() #remove excess whitespace
        return y;

hostname = "localhost"
username = "postgres"
password = "postgres"
database = "politics"

conn = psycopg2.connect(
    host = hostname,
    user = username,
    password = password,
    dbname = database)
cur = conn.cursor()

#Write a select query for id, first_name, and last_name for each affected table

select_pols = """select id, first_name, last_name from politicians
                 order by id asc"""
cur.execute(select_pols)
list_pols = list(cur)

select_house = """select id, first_name, last_name from house"""
cur.execute(select_house)
list_house = pd.DataFrame(list(cur))
list_house_columns = ['id', 'first_name', 'last_name']

select_senate = """select id, first_name, last_name from senate"""
cur.execute(select_senate)
list_senate = pd.DataFrame(list(cur))
list_senate.columns = ['id', 'first_name', 'last_name']

select_nicknames = """select nickname, full_name from nicknames"""
cur.execute(select_nicknames)
list_nicknames = pd.DataFrame(list(cur))
list_nicknames.columns = ['nickname', 'full_name']

#Run revised name formula on each person in a table
#Write an update query for that one line where the ids are equal
#Do this for all line items in the table

def find_suffix(a, b):
    if len(re.findall('(Sr.|Jr.|III|IV)', a)) > 0:
        m = re.search('(Sr.|Jr.|III|IV)', a)
        suff = m.string[m.start():m.end()]
        return suff
    elif len(re.findall('(Sr.|Jr.|III|IV)', b)) > 0:
        m = re.search('(Sr.|Jr.|III|IV)', b)
        suff = m.string[m.start():m.end()]
        return suff
    else:
        return None;

def find_nickname(a):
    for pol in a:
        pol_id = pol[0]
        first_name = pol[1]
        if first_name in list(list_nicknames['nickname']):
            full_name = list_nicknames[list_nicknames['nickname'] == first_name].iloc[0,1]
            nickname = first_name
            print(pol_id, full_name, nickname)
            update_query = """update politicians set 
            first_name = %s,
            nickname = %s
            where id = %s"""
            packet = (full_name, nickname, pol_id)
            cur.execute(update_query, vars = packet)
            conn.commit()
        else:
            pass;
        
find_nickname(list_pols)

# for i in list_pols:
#     update_query = """update politicians set 
#     id = %s, 
#     first_name = %s,
#     last_name = %s, 
#     suffix = %s
#     where id = %s"""
#     
#     fn = scrub_name(i[1])
#     ln = scrub_name(i[2])
#     suffix = find_suffix(i[1], i[2])
#     packet = (i[0], fn, ln, suffix, i[0])
#     cur.execute(update_query, vars = packet)
#     conn.commit()
#     print(packet);
     
# for i in list_house:
#     update_query = """update house set 
#     id = %s, 
#     first_name = %s,
#     last_name = %s, 
#     suffix = %s
#     where id = %s"""
#     update_pols = """update politicians set 
#     suffix = %s
#     where id = %s"""
#          
#     fn = scrub_name(i[1])
#     ln = scrub_name(i[2])
#     suffix = find_suffix(i[1], i[2])
#     packet = (i[0], fn, ln, suffix, i[0])
#     pol_packet = (suffix, i[0])
# #     cur.execute(update_query, vars = packet)
# #     cur.execute(update_pols, vars = pol_packet)
# #     conn.commit()
#     print(packet);
#  
# for i in list_senate:
#     update_query = """update senate set 
#     id = %s, 
#     first_name = %s,
#     last_name = %s, 
#     suffix = %s
#     where id = %s"""
#     update_pols = """update politicians set 
#     suffix = %s
#     where id = %s"""    
#      
#     fn = scrub_name(i[1])
#     ln = scrub_name(i[2])
#     suffix = find_suffix(i[1], i[2])
#     packet = (i[0], fn, ln, suffix, i[0])
#     pol_packet = (suffix, i[0])
# #     cur.execute(update_query, vars = packet)
# #     cur.execute(update_pols, vars = pol_packet)
# #     conn.commit()
#     print(packet);

#Repeat the above code for each affected table

#Close connection

cur.close()
conn.close()