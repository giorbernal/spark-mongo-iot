# spark-mongo-iot
Sample project to integrate spark with mongo database in an IoT context.
execute analytics of the previously populated collection **c_ed_datastreams** (by *DataLoader* application, incuded here) by using this command:

Example for LoadToHadoop:
```sh
spark-submit \
 --class es.bernal.sparkmongoiot.LoadToHadoop \
 --master spark://172.19.18.64:6066 \
 --deploy-mode cluster \
 --supervise \
 --total-executor-cores 12 \
 --executor-memory 4G \
/data/analytics-0.5.jar  \
0.25 172.19.18.77 odmtest ed_datapoints odmtest amplia MONGO
```

Example for Analtics:
```sh
spark-submit \
 --class es.bernal.sparkmongoiot.Analytic \
 --master spark://172.19.18.64:6066 \
 --deploy-mode cluster \
 --supervise \
 --total-executor-cores 12 \
 --executor-memory 5G \
 --driver-memory 4G \
 /data/analytics-0.5.jar \
 0.25 172.19.18.77 odmtest c_ed_analytics_datapoints_hadoop_15m odmtest amplia apolo
```

