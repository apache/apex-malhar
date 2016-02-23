Apex Malhar Changelog
========================================================================================================================

Version 3.3.0-incubating - 2016-01-10
------------------------------------------------------------------------------------------------------------------------

### Sub-task
* [APEXMALHAR-1877] - Move org.apache.hadoop.io.file.tfile from contrib to library in Malhar
* [APEXMALHAR-1901] - Test- DTFileTest creates test folder under lib directory
* [APEXMALHAR-1902] - Rename IdempotentStorage Manager
* [APEXMALHAR-1910] - Fix existing checkstyle violations in BlockReader and FileSplitter
* [APEXMALHAR-1912] - Fix existing check style violations in FileOutput, JMSInput, FTPInput, JDBC classes
* [APEXMALHAR-1916] - Add FileAccess API and its DTFileImplementation
* [APEXMALHAR-1931] - Augment FileAccess API
* [APEXMALHAR-1941] - Add a default Slice comparator to Malhar/util
* [APEXMALHAR-1943] - Add Aggregator to Malhar and make it top level interface
* [APEXMALHAR-1944] - Add DimensionsConversionContext to Malhar and make it top class
* [APEXMALHAR-1945] - Upgrade the version of japicmp to 0.6.2

### Bug
* [APEXMALHAR-1880] - Incorrect documentation for maxLength property on AbstractFileOutputOperator
* [APEXMALHAR-1887] - shutdown field in WebSocketInputOperator should be volatile
* [APEXMALHAR-1894] - Add an Input Port With An isConnected Method
* [APEXMALHAR-1922] - FileStreamContext - Set filterStream variable to transient
* [APEXMALHAR-1925] - The kafka offset manager may not store the offset of processed data in all scenarios
* [APEXMALHAR-1928] - Update checkpointed offsettrack in operator thread instead of consumer thread
* [APEXMALHAR-1929] - japicmp plugin fails for malhar samples
* [APEXMALHAR-1934] - When offset is unavailable kafka operator stops reading data
* [APEXMALHAR-1949] - JDBC Input Operator unnecessarily waits two times when the result is empty
* [APEXMALHAR-1960] - Test failure KafkaInputOperatorTest.testRecoveryAndIdempotency

### Improvement
* [APEXMALHAR-1895] - Refactor Snapshot Server
* [APEXMALHAR-1896] - Add Utility Functions For Working With Schema Tags
* [APEXMALHAR-1906] - Snapshot Server support tags
* [APEXMALHAR-1908] - Add Deserialization Function That Deserializes keys with multiple values
* [APEXMALHAR-1913] - FileSplitter - Need access to modifiedTime of ScannedFileInfo class
* [APEXMALHAR-1918] - FileSplitter - Need stopScanning method in Scanner
* [APEXMALHAR-1940] - Create Operator Utility Class Which Converts Time To Windows
* [APEXMALHAR-1958] - Provide access to doneTuple field in AbstractReconciler for derived classes

### New Feature
* [APEXMALHAR-1812] - Support Anti Join
* [APEXMALHAR-1813] - Support Semi Join
* [APEXMALHAR-1904] - New Kafka input operator using 0.9.0 consumer APIs

### Task
* [APEXMALHAR-1859] - Integrate checkstyle with Malhar
* [APEXMALHAR-1892] - Fix missing javadoc
* [APEXMALHAR-1905] - Test the old kafka input operator is compatible with 0.9.0 broker
* [APEXMALHAR-1950] - Identify and mark Operators and Components as @Evolving
* [APEXMALHAR-1956] - Concrete generic Implementation of Kafka Output Operator with auto metrics and batch processing
* [APEXMALHAR-1964] - Checkstyle - Reduce the severity of line length check

Version 3.2.0-incubating - 2015-11-13
------------------------------------------------------------------------------------------------------------------------

### Sub-task
* [MLHR-1870] - JsonParser unit test failing
* [MLHR-1872] - Add license headers in unit tests of parsers and formatters
* [MLHR-1886] - Optimize recovery of files which are not corrupted
* [MLHR-1889] - AbstractFileOutputOperator should have rename method to do rename operation

### Bug
* [MLHR-1799] - Cassandra Pojo input operator is broken
* [MLHR-1820] - Fix NPE in SnapshotServer
* [MLHR-1823] - AbstractFileOutputOperator not finalizing the file after the recovery
* [MLHR-1825] - AbstractFileOutputOperator throwing FileNotFoundException during the recovery
* [MLHR-1830] - Fix Backword Compatibility Errors
* [MLHR-1835] - WebSocketInputOperator Creates More And More Zombie Threads As It Runs
* [MLHR-1837] - AbstractFileOutputOperator writing to same temp file after the recovery
* [MLHR-1839] - Configure All The Twitter Demos To Use Embeddable Query
* [MLHR-1841] - AbstractFileOutputOperator rotation interval not working when there is no processing
* [MLHR-1852] - File Splitter Test Failing On My Machine
* [MLHR-1856] - Make Custom Time Buckets Sortable
* [MLHR-1860] - Check for null fileName in new wordcount app in wrong place
* [MLHR-1864] - Some Times Expired Queries Are processed
* [MLHR-1866] - Travis-ci build integration
* [MLHR-1876] - WindowBoundedService Can Block The Shutdown Of A Container
* [MLHR-1880] - Incorrect documentation for maxLength property on AbstractFileOutputOperator
* [MLHR-1885] - Adding getter methods to the variables of KafkaMessage

### Task
* [MLHR-1857] - Apache license headers and related files
* [MLHR-1869] - Update Maven coordinates for ASF release
* [MLHR-1871] - Expand checks in CI build
* [MLHR-1891] - Skip install/deploy of source archives

### Improvement
* [MLHR-1803] - Add Embeddable Query To AppDataSnapshotServer
* [MLHR-1804] - Enable FileSplitter to be used as a non-input operator
* [MLHR-1805] - Ability to supply additional file meta information in FileSplitter
* [MLHR-1806] - Ability to supply additional block meta information in FileSplitter
* [MLHR-1824] - Convert Pi Demo to support Query Operator
* [MLHR-1836] - Integrate schema with Jdbc POJO operators
* [MLHR-1862] - Clean up code for Machine Data Demo
* [MLHR-1863] - Make Custom Time Bucket Comparable
* [MLHR-1868] - Improve GPOUtils hashcode function

