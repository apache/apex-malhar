# Apache Apex Example (NYC Taxi Data)

## Overview

This is an example that demonstrates how Apex can be used for processing ride service data, using the freely available
historical Yellow Cab trip data on New York City government's web site.

It uses concepts of event-time windowing, out-of-order processing and streaming windows.

## Instructions

### Data preparation
Download some Yellow Cab trip data CSV files from the nyc.gov website.

Let's say the data is saved as yellow_tripdata_2016-01.csv.

Because the trip data source is wildly unordered, sort the data with some random deviation.
```bash
bash> sort -t, -k2 yellow_tripdata_2016-01.csv > yellow_tripdata_sorted_2016-01.csv
```

Then add some random deviation to the sorted data:

```bash
bash> cat nyctaxidata/yellow_tripdata_sorted_2016-01.csv | perl -e '@lines = (); while (<>) { if (@lines && rand(10) < 1) { print shift @lines;  } if (rand(50) < 1) { push @lines, $_; } else { print $_; } }' > yellow_tripdata_sorted_random_2016-01.csv
```

Then create an HDFS directory and copy the csv file there:

```bash
bash> hdfs dfs -mkdir nyctaxidata
bash> hdfs dfs -copyFromLocal yellow_tripdata_sorted_random_2016-01.csv nyctaxidata/
```

### Setting up pubsub server

bash> git clone https://github.com/atrato/pubsub-server

Then build and run the pubsub server (the message broker):

bash> cd pubsub-server; mvn compile exec:java

The pubsub server is now running, listening to the default port 8890 on localhost.

### Running the application

Open the Apex CLI command prompt and run the application:

```bash
bash> apex
apex> launch target/malhar-examples-nyc-taxi-3.8.0-SNAPSHOT.apa
```

After the application has been running for 5 minutes, we can start querying the data. The reason why we need to wait
5 minutes is because we need to wait for the first window to pass the watermark for the triggers to be fired by the
WindowedOperator. Subsequent triggers will be fired every one minute since the slideBy is one minute.

We can use the Simple WebSocket Client Google Chrome extension to query the data. Open the extension in Chrome and
connect to "ws://localhost:8890/pubsub". Subscribe to the query result topic first because results to any query will be
delivered to this topic by sending this to the websocket connection:

```json
{"type":"subscribe","topic":"nyctaxi.result"}
```

Issue a query with latitude/longitude somewhere in Manhattan:

```json
{"type":"publish","topic":"nyctaxi.query","data":{"lat":40.731829, "lon":-73.989181}}
```

You should get back something like the following:

```json
{"type":"data","topic":"nyctaxi.result","data":{"currentZip":"10003","driveToZip":"10011"},"timestamp":1500769034523}
```

The result to the same query changes as time goes by since we have "real-time" ride data coming in:
```json
{"type":"publish","topic":"nyctaxi.query","data":{"lat":40.731829, "lon":-73.989181}}
{"type":"data","topic":"nyctaxi.result","data":{"currentZip":"10003","driveToZip":"10003"},"timestamp":1500769158530}
{"type":"publish","topic":"nyctaxi.query","data":{"lat":40.731829, "lon":-73.989181}}
{"type":"data","topic":"nyctaxi.result","data":{"currentZip":"10003","driveToZip":"10011"},"timestamp":1500769827538}
{"type":"publish","topic":"nyctaxi.query","data":{"lat":40.731829, "lon":-73.989181}}
{"type":"data","topic":"nyctaxi.result","data":{"currentZip":"10003","driveToZip":"10012"},"timestamp":1500770540527}
```

