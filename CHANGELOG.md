Apex Malhar Changelog
========================================================================================================================


Version 3.2.0-incubating - 2015-11-08
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

