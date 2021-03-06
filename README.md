Elasticize
==========

Elasticize is a simple way to transfer data from MongoDB to Elasticsearch.



Setup
-----
Use the config.json file to specify the source and destination of your data

```json
{
	"MONGO_HOST": "127.0.0.1",
	"MONGO_PORT": "27017",
	"MONGO_AUTH": false,
	"MONGO_USER": "source_mongo_username",
	"MONGO_PASSWORD": "source_mongo_password",
	"MONGO_DATABASE": "source_mongo_db",
	"MONGO_COLLECTION": "source_mongo_collection",
	"ES_URL": "http://localhost:9200",
	"ES_INDEX": "destination_index",
	"ES_TYPE" : "destination_type"
}
```

Run
---
Once the configuration is done, just run:

```python
python elasticize.py [-h] [-f FILTER] [-o OUTFILE]
```
### Arguments

#### FILTER
The FILTER must be a JSON object as string which will be applied when querying the source collection. It even works with dates. For example:
```python
python elasticize.py -f "{ \"timeUtc\": { \"$gt\": \"2017-03-24T10:41:18.887Z\" } }"
```
When Elasticize detects an iso time string it will automatically convert it to a datetime object. This lets you bypass the ISODate() call within the JSON object.

#### OUTFILE
The OUTFILE can be a filename in which the total number of read and moved documents will be written. If it will be omitted no file will be created.

Dependencies
------------

Elasticize is written in Python. So, you need to install Python on your system if not already installed.
Other few dependencies are needed as **Requests**, an http request python library to send request to ES and **Pymongo** to fetch data from a MongoDB database.

Here's a list of the dependecies needed:

- [Python](https://www.python.org/downloads/) 2.7
- [Requests](http://docs.python-requests.org/en/master/user/install/#install)
- [Pymongo](https://api.mongodb.org/python/current/installation.html)
- [Isodate](https://pypi.python.org/pypi/isodate)
- [Elasticsearch](https://pypi.python.org/pypi/elasticsearch)
