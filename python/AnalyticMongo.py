# install pymongo
# pip install pymongo

from pymongo import MongoClient
from urllib.parse import quote_plus
from time import time
from datetime import datetime

host = "172.19.18.77"
user = "odmtest"
password = "amplia"
dbConf = "odmtest"
collectionInConf = "c_ed_datapoints"
collectionOutConf = "c_ed_analytics_datapoints_mongo"
uri = "mongodb://%s:%s@%s/%s.%s" % (quote_plus(user), quote_plus(password), host, dbConf, collectionInConf)
mongoClient = MongoClient(uri)

# DataBase Connection
db = mongoClient.odmtest

# Retrieve collections
collectionIn = db.c_ed_datapoints
collectionOut = db.c_ed_analytics_datapoints_mongo

print("Starting application ... ")
timeini = time()

# getter test
#singleJson = collection.find_one()
#print(singleJson)

blockSize=1000

# TODO I don't know how to insert variables in this objets ... :_(
# timeThreshold = 1296025107432 # complete
# timeThreshold = 1296782999628 # 1 day
# timeThreshold = 1296865799628 # 1 hour
# timeThreshold = 1296868499628 # 15 minutes

pipelineContinuousCoverage = [
						{
						    "$match": { "date.epoch":{"$gt":1296025107432}, "datastreamId": "coverage" }
						},
						{
						    "$group": { "_id": { "deviceId": "$deviceId", "datastreamId": "$datastreamId", "organizationId": "$organizationId", "channelId": "$channelId", }, "count": { "$sum": 1}, "avg": { "$avg": "$value"}, "max": { "$max": "$value"}, "min": { "$min": "$value"} }
						}
					]

pipelineDiscreteManufacturer = [
				{
				    "$match": { "date.epoch":{"$gt":1296025107432}, "datastreamId": "manufacturer" }
				},
				{
				    "$group": { "_id": { "deviceId": "$deviceId", "datastreamId": "$datastreamId", "organizationId": "$organizationId", "channelId": "$channelId", "value": "$value" }, "count": { "$sum": 1} }
				},
				{
				    "$group": { "_id": { "deviceId": "$_id.deviceId", "datastreamId": "$_id.datastreamId", "organizationId": "$_id.organizationId", "channelId": "$_id.channelId" }, "accumulators": { "$addToSet": {"name":"$_id.value", "count":"$count"} } }
				}
			]

pipelineDiscretePresence = [
				{
				    "$match": { "date.epoch":{"$gt":1296025107432} , "datastreamId": "presence"}
				},
				{
				    "$group": { "_id": { "deviceId": "$deviceId", "datastreamId": "$datastreamId", "organizationId": "$organizationId", "channelId": "$channelId", "value": "$value" }, "count": { "$sum": 1} }
				},
				{
				    "$group": { "_id": { "deviceId": "$_id.deviceId", "datastreamId": "$_id.datastreamId", "organizationId": "$_id.organizationId", "channelId": "$_id.channelId" }, "accumulators": { "$addToSet": {"name":"$_id.value", "count":"$count"} } }
				}
			]

pipelineDiscreteImei = [
				{
				    "$match": { "date.epoch":{"$gt":1296025107432}, "datastreamId": "imei" }
				},
				{
				    "$group": { "_id": { "deviceId": "$deviceId", "datastreamId": "$datastreamId", "organizationId": "$organizationId", "channelId": "$channelId", "value": "$value" }, "count": { "$sum": 1} }
				},
				{
				    "$group": { "_id": { "deviceId": "$_id.deviceId", "datastreamId": "$_id.datastreamId", "organizationId": "$_id.organizationId", "channelId": "$_id.channelId" }, "accumulators": { "$addToSet": {"name":"$_id.value", "count":"$count"} } }
				}
			]

pipelineDiscreteLocation = [
				{
				    "$match": { "date.epoch":{"$gt":1296025107432}, "datastreamId": "location" }
				},
				{
				    "$group": { "_id": { "deviceId": "$deviceId", "datastreamId": "$datastreamId", "organizationId": "$organizationId", "channelId": "$channelId", "value": "$value" }, "count": { "$sum": 1} }
				},
				{
				    "$group": { "_id": { "deviceId": "$_id.deviceId", "datastreamId": "$_id.datastreamId", "organizationId": "$_id.organizationId", "channelId": "$_id.channelId" }, "accumulators": { "$addToSet": {"name":"$_id.value", "count":"$count"} } }
				}
			]

pipelineDiscreteImsi = [
				{
				    "$match": { "date.epoch":{"$gt":1296025107432}, "datastreamId": "imsi" }
				},
				{
				    "$group": { "_id": { "deviceId": "$deviceId", "datastreamId": "$datastreamId", "organizationId": "$organizationId", "channelId": "$channelId", "value": "$value" }, "count": { "$sum": 1} }
				},
				{
				    "$group": { "_id": { "deviceId": "$_id.deviceId", "datastreamId": "$_id.datastreamId", "organizationId": "$_id.organizationId", "channelId": "$_id.channelId" }, "accumulators": { "$addToSet": {"name":"$_id.value", "count":"$count"} } }
				}
			]

pipelineDiscreteIcc = [
				{
				    "$match": { "date.epoch":{"$gt":1296025107432}, "datastreamId": "icc" }
				},
				{
				    "$group": { "_id": { "deviceId": "$deviceId", "datastreamId": "$datastreamId", "organizationId": "$organizationId", "channelId": "$channelId", "value": "$value" }, "count": { "$sum": 1} }
				},
				{
				    "$group": { "_id": { "deviceId": "$_id.deviceId", "datastreamId": "$_id.datastreamId", "organizationId": "$_id.organizationId", "channelId": "$_id.channelId" }, "accumulators": { "$addToSet": {"name":"$_id.value", "count":"$count"} } }
				}
			]


dss = collectionIn.distinct("datastreamId")
for ds in dss:
	print("process for datastreamId: " + ds)
	i=0
	bulk = collectionOut.initialize_unordered_bulk_op()
	if ds=='coverage':
		cursor = collectionIn.aggregate(pipelineContinuousCoverage)
		for dsdoc in cursor:
			#print("doc A:" + str(dsdoc))
			bulk.insert(dsdoc)
			i = i+1
			if i%blockSize == 0:
				#print("write data block ...")
				try:
					bulk.execute()
				except BulkWriteError as bwe:
					pprint(bwe.details)
				i=0
				bulk = collectionOut.initialize_unordered_bulk_op()
	elif ds == 'manufacturer':
		cursor = collectionIn.aggregate(pipelineDiscreteManufacturer)
		for dsdoc in cursor:
			#print("doc A:" + str(dsdoc))
			bulk.insert(dsdoc)
			i = i+1
			if i%blockSize == 0:
				#print("write data block ...")
				try:
					bulk.execute()
				except BulkWriteError as bwe:
					pprint(bwe.details)
				i=0
				bulk = collectionOut.initialize_unordered_bulk_op()
	elif ds == 'presence':
		cursor = collectionIn.aggregate(pipelineDiscretePresence)
		for dsdoc in cursor:
			#print("doc A:" + str(dsdoc))
			bulk.insert(dsdoc)
			i = i+1
			if i%blockSize == 0:
				#print("write data block ...")
				try:
					bulk.execute()
				except BulkWriteError as bwe:
					pprint(bwe.details)
				i=0
				bulk = collectionOut.initialize_unordered_bulk_op()
	elif ds == 'imei':
		cursor = collectionIn.aggregate(pipelineDiscreteImei)
		for dsdoc in cursor:
			#print("doc A:" + str(dsdoc))
			bulk.insert(dsdoc)
			i = i+1
			if i%blockSize == 0:
				#print("write data block ...")
				try:
					bulk.execute()
				except BulkWriteError as bwe:
					pprint(bwe.details)
				i=0
				bulk = collectionOut.initialize_unordered_bulk_op()
	elif ds == 'location':
		cursor = collectionIn.aggregate(pipelineDiscreteLocation)
		for dsdoc in cursor:
			#print("doc A:" + str(dsdoc))
			bulk.insert(dsdoc)
			i = i+1
			if i%blockSize == 0:
				#print("write data block ...")
				try:
					bulk.execute()
				except BulkWriteError as bwe:
					pprint(bwe.details)
				i=0
				bulk = collectionOut.initialize_unordered_bulk_op()
	elif ds == 'imsi':
		cursor = collectionIn.aggregate(pipelineDiscreteImsi)
		for dsdoc in cursor:
			#print("doc A:" + str(dsdoc))
			bulk.insert(dsdoc)
			i = i+1
			if i%blockSize == 0:
				#print("write data block ...")
				try:
					bulk.execute()
				except BulkWriteError as bwe:
					pprint(bwe.details)
				i=0
				bulk = collectionOut.initialize_unordered_bulk_op()
	elif ds == 'icc':
		cursor = collectionIn.aggregate(pipelineDiscreteIcc)
		for dsdoc in cursor:
			#print("doc A:" + str(dsdoc))
			bulk.insert(dsdoc)
			i = i+1
			if i%blockSize == 0:
				#print("write data block ...")
				try:
					bulk.execute()
				except BulkWriteError as bwe:
					pprint(bwe.details)
				i=0
				bulk = collectionOut.initialize_unordered_bulk_op()
	if i>0:
		#print("write last data block")
		try:
			bulk.execute()
		except BulkWriteError as bwe:
			pprint(bwe.details)

timeend = time()
deltatime = timeend - timeini

print("\nTotal time " + str(deltatime) + " s\n")

# Close connection
mongoClient.close()
