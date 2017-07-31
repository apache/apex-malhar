# Examples for end-to-end exactly-once

## Read from Kafka, write to JDBC

This application shows exactly-once output to JDBC through transactions:

[Application](src/main/java/org/apache/apex/examples/exactlyonce/ExactlyOnceJdbcOutputApp.java)

[Test](src/test/java/org/apache/apex/examples/exactlyonce/ExactlyOnceJdbcOutputTest.java)

## Read from Kafka, write to Files

This application shows exactly-once output to HDFS through atomic file operation:

[Application](src/main/java/org/apache/apex/examples/exactlyonce/ExactlyOnceFileOutputApp.java)

[Test](src/test/java/org/apache/apex/examples/exactlyonce/ExactlyOnceFileOutputAppTest.java)
