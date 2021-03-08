import json
#import logging
#import os
import time
import http.client
from sql_database import SQLDatabase




baseUrl = "/api/search-qf?searchkey=SEARCH_ID_JOB_FULLTIME&location="
urlList = [
    "0.20001",
    "1.20001.22042",
    "1.20001.22034",
    "1.20001.20015",
    "1.20001.20018",
    "1.20001.20061",
    "1.20001.20012",
    "1.20001.22054",
    "1.20001.20016",
    "1.20001.22038",
    "1.20001.22046",
    "1.20001.22030"
    ]

urlList = [
    "0.20001"
    ]

ts = time.gmtime()
timestamp = time.strftime("%Y-%m-%d", ts)

db = SQLDatabase()

for location in urlList:
    connection = http.client.HTTPSConnection("www.finn.no")
    connection.request("GET", baseUrl+location)
    response = connection.getresponse()
    
    event = json.loads( response.read().decode('utf-8')  )
    connection.close()
    
    tables_dict = {
        'occupation' : event['filters'][3]['filter_items'],
        'industry'   : event['filters'][4]['filter_items'],
        'duration'   : event['filters'][6]['filter_items'],
        'form'       : event['filters'][7]['filter_items'],
        'sector'     : event['filters'][8]['filter_items'],
        'role'       : event['filters'][9]['filter_items']
    }
    metadata = event['metadata']

    date = timestamp
    location = metadata['selected_filters'][0]['display_name']
    #total_positions = metadata['result_size']['match_count']
    total_ads = metadata['result_size']['group_count']

    public_table = "public.occupation"
    columns = "category, amount, date, location"
    values = "'%s', %s, '%s', '%s'" %('Alle yrker', total_ads, date, location)
    #db.insert_data(public_table, columns, values)

    for table_name, table in tables_dict.items():
        #print(table)
        for row in table:
            #print(table_name, date, location, row['display_name'], row['hits'])
            #occupationDict.update( {occupations[i]['display_name']: occupations[i]['hits']})
            public_table = table_name
            columns = "category, amount, date, location"
            values = "'%s', %s, '%s', '%s'" %(row['display_name'], row['hits'], date, location)
            #db.insert_data(public_table, columns, values)
            print(values)


db.disconnect()
        



