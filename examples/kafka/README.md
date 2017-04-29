## Kafka to HDFS example :

This sample application show how to read lines from a Kafka topic using the new (0.9)
Kafka input operator and write them out to HDFS using rolling files with a bounded size.

The output files start out with a `.tmp` extension and get renamed when they reach the
size bound.  Additional operators to perform parsing, aggregation or filtering can be
inserted into this pipeline as needed.

## HDFS to Kafka example :

This sample application shows how to read lines from files in HDFS and write
them out to a Kafka topic. Each line of the input file is considered a separate
message. The topic name, the name of the directory that is monitored for input
files, and other parameters are configurable in `META_INF/properties-hdfs2kafka.xml`.

