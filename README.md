# hafka

A http wrapper of kafka, simple but efficient

## usage

Run server
```shell
hafka -port 8000 -server s1:9092,s2:9092 [-consumer api1:topic1;api2:topic21,topic22] [-producer api1:topic1;api2:topic2] [-prefix prefix]
```
Parameters can also be set by ENVIRONMENT variables.

Client produce message to topic **topic1**
```shell
curl -x POST -d "data" http://localhost:8000/prefix/feed/api1 
```

Client cosume message from topic **topic1**
```shell
curl -x GET http://localhost:8000/prefix/poll/api1 
```



