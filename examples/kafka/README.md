This sample application show how to read lines from a Kafka topic using the new (0.9)
Kafka input operator and write them out to HDFS using rolling files with a bounded size.

The output files start out with a `.tmp` extension and get renamed when they reach the
size bound.  Additional operators to perform parsing, aggregation or filtering can be
inserted into this pipeline as needed.
