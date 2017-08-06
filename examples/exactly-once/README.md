# Examples for end-to-end exactly-once

The examples are a variation of word count to illustrate end-to-end exactly-once processing
by incorporating the external system integration aspect, which needs to be taken into account when
developing real-world pipelines:

* Read from Kafka source
* Windowed count aggregation that emits incremental aggregates
* Sink that maintains totals accumulating the incremental aggregates (shown for JDBC and file output)

The examples combine the 3 properties that are required for end-to-end exactly-once results:

1. At-least-once processing that guarantees no loss of data
2. Idempotency in the DAG (Kafka input operator and repeatable/deterministic streaming windows)
3. Consistent state between DAG and external system, enabled by the output operators.

The test cases show how the applications can be configured to run in embedded mode (including Kafka).

## Read from Kafka, write to JDBC

Shows exactly-once output to JDBC through transactions. The JDBC output operator
keeps track of the streaming window along with the count to avoid duplicate writes on replay
during recovery. This is an example for continuously updating results in the database,
enabled by the transactions.

[Application](src/main/java/org/apache/apex/examples/exactlyonce/ExactlyOnceJdbcOutputApp.java)

[Test](src/test/java/org/apache/apex/examples/exactlyonce/ExactlyOnceJdbcOutputTest.java)

## Read from Kafka, write to Files

This application shows exactly-once output to files through atomic file operation. In contrast to the
JDBC example, output can only occur once the final count is computed. This implies batching at the sink,
leading to high latency.

[Application](src/main/java/org/apache/apex/examples/exactlyonce/ExactlyOnceFileOutputApp.java)

[Test](src/test/java/org/apache/apex/examples/exactlyonce/ExactlyOnceFileOutputAppTest.java)
