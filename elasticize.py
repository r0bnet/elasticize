import requests
import pymongo
import sys
import re
import isodate
from pymongo import MongoClient
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
config = json.loads(open('config.json', 'r').read())

def is_iso_date_string(date_str):
	iso_date_pattern = '^(\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z))|(\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d([+-][0-2]\d:[0-5]\d|Z))|(\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d([+-][0-2]\d:[0-5]\d|Z))$'
	res = re.search(iso_date_pattern, date_str)
	return True if res != None else False

def convert_date_strings_to_dates(obj):
	for key in obj:
		if isinstance(obj[key], (str, unicode)):
			if is_iso_date_string(obj[key]):
				obj[key] = isodate.parse_datetime(obj[key])
		elif isinstance(obj[key], dict):
			convert_date_strings_to_dates(obj[key])

collectionfilter = {};
if len(sys.argv) > 1:
	try:
		collectionfilter = json.loads(sys.argv[1])
	except:
		print('Could not parse collection filter')
		pass

convert_date_strings_to_dates(collectionfilter)		

connection = pymongo.MongoClient()
db = connection[config['MONGO_DATABASE']]
es = Elasticsearch(config['ES_URL'])

def outputJSON(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%dT%H:%M:%S')

collection = db[config['MONGO_COLLECTION']].find(collectionfilter)
documents = []

print("Creating documents...")
for n in range(0, collection.count()):
    record = collection.next()
    _id = str(record['_id'])
    map(record.pop, ['_id'])
    document = {    "_index": config['ES_INDEX'],
                    "_type": config['ES_TYPE'],
                    "_id": _id,
                    "_source": json.dumps(record, default=outputJSON)
                }
    documents.append(document)

print("{0} documents created, starting bulk inserting...").format(len(documents))
helpers.bulk(es,documents, stats_only=True, request_timeout= 320)
