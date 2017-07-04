# spark-mongo-iot
Sample project to integrate spark with mongo database (and Hadoop) in an IoT context.
In this projects there are 4 diferent small applications for testing porpouses, All of them work with certain params, when they does not exists, the application work in a default mode to be tested in local environment. After assembly (sbt assembly), a .jar that can be load in spark cluster will be generated in target. Later, applications could be executed in spark cluster environemt as next examples show.

## DataLoader
Aplication to load Ed* data from .csv to datapoint info in **c_ed_datapoint** collection in mongo database. All .csv must be included at *resources/data* (.csv not included)

## LoadDataToHadoop
The application, obviously, load data from MongoDB to Hadoop, it can be do it by using both connectors (Hadoop MongoDB Connector / Spark MongoDB Connector) to compare earch other.
**Example 1:** Load last 24 hours data from my mongo database *mydb* located in 172.19.18.77, collection *c_ed_datapoints* (user: *myuser* / pwd: *mypwd*) and store data in Hadoop NFDS located in hadoopNfds:8020 by using the Spark MongoDB Connector
```sh
spark-submit \
 --class es.bernal.sparkmongoiot.LoadToHadoop \
 --master spark://172.19.18.64:6066 \
 --deploy-mode cluster \
 --supervise \
 --total-executor-cores 12 \
 --executor-memory 4G \
/data/analytics-0.7.jar  \
24 172.19.18.77 mydb ed_datapoints myuser mypwd hadoopNfds:8020 MONGO
```

**Example 2:** Just the same but using the Hadoop MongoDB Connector instead of the Spark MongoDB Connector
```sh
spark-submit \
 --class es.bernal.sparkmongoiot.LoadToHadoop \
 --master spark://172.19.18.64:6066 \
 --deploy-mode cluster \
 --supervise \
 --total-executor-cores 12 \
 --executor-memory 4G \
/data/analytics-0.7.jar  \
24 172.19.18.77 mydb ed_datapoints myuser mypwd hadoopNfds:8020 HADOOP
```

## Analytic
This will be the most important application of the project. It calculates aggregations for all continuous and discrete valued datastreams in the period to evaluate
**Example 1**: Calculate the aggregations of the data stored in hadoop, in file *ed_datapoints* located un NDFS in *hadoopNfds:8020* and store the results un mongo database/collection *mydb/c_ed_analytics_datapoints* located in 172.19.18.77 with user *myuser* and password *mypwd*. **First param '1' will bi ignored**
```sh
spark-submit \
 --class es.bernal.sparkmongoiot.Analytic \
 --master spark://172.19.18.64:6066 \
 --deploy-mode cluster \
 --supervise \
 --total-executor-cores 12 \
 --executor-memory 5G \
 --driver-memory 4G \
 /data/analytics-0.7.jar \
 1 172.19.18.77 mydb c_ed_analytics_datapoints myuser mypwd hadoopNfds:8020
```
**Example 2**: Calculate the aggregations of the last hour data stored in MongoDB, collection *c_ed_datapoints* located in 172.19.18.77 with user *myuser* and password *mypwd*, and save the results in collection *c_ed_analytics_datapoints* from the same database
```sh
spark-submit \
 --class es.bernal.sparkmongoiot.Analytic \
 --master spark://172.19.18.64:6066 \
 --deploy-mode cluster \
 --supervise \
 --total-executor-cores 12 \
 --executor-memory 5G \
 --driver-memory 4G \
 /data/analytics-0.7.jar \
 1 172.19.18.77 mydb c_ed_analytics_datapoints myuser mypwd MONGO
```
## SimpleMongoData
The simple application just read data from MongoDB by using again Spark Mongo Connector and clone it in another collection. This application searchs to look up the effects to avoid the *shuffling* issue.
