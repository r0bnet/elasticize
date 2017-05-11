import requests
import pymongo
import sys
import re
import isodate
import argparse
import urllib
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
			
parser = argparse.ArgumentParser()
parser.add_argument('-f', '--filter', help='Filter for the MongoDB collection', type=str, default='{}', required=False)
parser.add_argument('-o', '--outfile', help='File to write the number of documents which have been copied', default=None, type=str, required=False)
args = parser.parse_args()

collectionfilter = {};
try:
	collectionfilter = json.loads(args.filter)
except:
	print('Could not parse collection filter')
	pass

convert_date_strings_to_dates(collectionfilter)		

connstr = 'mongodb://' + (config['MONGO_USER'] + ':' + urllib.quote_plus(config['MONGO_PASSWORD']) + '@' if config['MONGO_AUTH'] else '') + \
	config['MONGO_HOST'] + ':' + config['MONGO_PORT'] + ('/' + config['MONGO_DATABASE'] if config ['MONGO_AUTH'] else '')
connection = pymongo.MongoClient(connstr)
db = connection[config['MONGO_DATABASE']]
es = Elasticsearch(config['ES_URL'])

def outputJSON(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%dT%H:%M:%S')

collection = db[config['MONGO_COLLECTION']].find(collectionfilter)
documents = []
print(collection.count())

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

if args.outfile is not None:	
	with open(args.outfile, 'wb') as fh:
		fh.write(str(collection.count()))
