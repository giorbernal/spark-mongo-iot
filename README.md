# spark-mongo-iot
Sample project to integrate spark with mongo database in an IoT context.
execute analytics of the previously populated collection **c_ed_datastreams** (by *DataLoader* application, incuded here) by using this command:

```sh
$ es.bernal.sparkmongoiot.Analytic <number_hours> <ip> <database> <output_collection> <user> <password>
```
