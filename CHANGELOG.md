Apex Malhar Changelog
========================================================================================================================

Version 3.8.0 - 2017-11-08
------------------------------------------------------------------------------------------------------------------------

### Sub-task
* [APEXMALHAR-2458] - Move kafka related examples from datatorrent examples to apex-malhar examples
* [APEXMALHAR-2462] - Move JDBC related examples from datatorrent examples to apex-malhar examples
* [APEXMALHAR-2474] - FSLoader only returns value at the beginning
* [APEXMALHAR-2475] - CacheStore needn't expire data if it read-only data
* [APEXMALHAR-2480] - Move Amazon S3 related examples from datatorrent examples to apex-malhar examples

### Bug
* [APEXMALHAR-2397] - Malhar demo application PiDemoAppData fails apex get-app-package-info command
* [APEXMALHAR-2404] - Avro file input operator should call beginWindow of AbstractFileInputOperator
* [APEXMALHAR-2434] - JMSTransactionableStore uses Session.createQueue() which fails
* [APEXMALHAR-2460] - Redshift output module tuples unable to emit tuples
* [APEXMALHAR-2461] - Fix dependencies on libraries licensed under Category X
* [APEXMALHAR-2465] - Travis build is failing because of excessive logging
* [APEXMALHAR-2476] - Unable to override tupleSeparator property of GenericFileOutputOperator using configurations
* [APEXMALHAR-2481] - High Level Stream API does not support lambda expressions
* [APEXMALHAR-2491] - GenericFileOutputOperatorTest : runTestApplication() does not call shutdown() and can continue to run.
* [APEXMALHAR-2492] - Correct usage of empty Slice in Malhar Library
* [APEXMALHAR-2493] - KafkaSinglePortExactlyOnceOutputOperator going to the blocked state during recovery
* [APEXMALHAR-2500] - Code comment for FSInputModule mentions "...is abstract class..." but the class is not abstract
* [APEXMALHAR-2502] - Fix KuduOutput operator for extensibility
* [APEXMALHAR-2505] - SnapshotSchema should retain original field order
* [APEXMALHAR-2508] - CacheManager throws exception when the operator receives a shutdown request
* [APEXMALHAR-2513] - JDBCPollInputOperator issues
* [APEXMALHAR-2518] - Kafka input operator stops reading tuples when there is a UNKNOWN_MEMBER_ID error during committed offset processing
* [APEXMALHAR-2519] - Performance Benchmark containers stop repeatedly as they exceed physical memory allocated
* [APEXMALHAR-2525] - YahooFinance example crashes with java.lang.NumberFormatException because YHOO doesn't exist anymore
* [APEXMALHAR-2526] - FunctionOperator deserialization failure in CLI
* [APEXMALHAR-2532] - Transform Application Test flooding CI logs
* [APEXMALHAR-2533] - In TopNWordsWithQueries example, input data from individual files is sorted and written in a single file instead of different files.
* [APEXMALHAR-2534] - In TopNWordsWithQueries example, input data from individual files is sorted and written in a single file instead of different files.
* [APEXMALHAR-2535] - Timeouts in AbstractEnricher specified as int which limits duration of time which could be specified.
* [APEXMALHAR-2540] - Serialization exception with throttle example
* [APEXMALHAR-2542] - enricher.ApplicationTest access to hard coded /tmp location
* [APEXMALHAR-2544] - Flume test leads to Travis timeout

### Dependency upgrade
*  [APEXMALHAR-2516] - Fix few dependency issues in malhar-contrib

### Documentation
* [APEXMALHAR-2384] - Add documentation for FixedWIdthParser
* [APEXMALHAR-2457] - Duplicate entries in navigation panel

### Improvement
* [APEXMALHAR-2324] - Add support for flume
* [APEXMALHAR-2366] - Apply BloomFilter to Bucket
* [APEXMALHAR-2435] - specify KeyedWindowedOperatorBenchmarkApp application name
* [APEXMALHAR-2447] - No indication from AbstractFileInputOperator when directory is empty
* [APEXMALHAR-2473] - Support for global cache meta information in db CacheManager
* [APEXMALHAR-2487] - Malhar should support outputting data in Snappy compression
* [APEXMALHAR-2489] - Change algorithm for average calculation in RunningAverage
* [APEXMALHAR-2494] - Update demo apps with description
* [APEXMALHAR-2504] - Allow for customization of TwitterSampleInput ConfigurationBuilder
* [APEXMALHAR-2506] - Kafka Input operator - needs better handling of failure of Kafka reader thread
* [APEXMALHAR-2514] - JDBCPollInputOperator support for offset rebase
* [APEXMALHAR-2515] - HBase output operator Multi Table feature.
* [APEXMALHAR-2529] - Allow subclass of AbstractFileInputOperator to control when to advance streaming window
* [APEXMALHAR-2530] - Refactor AbstractAppDataSnapshotServer so that subclasses don't need schemas
* [APEXMALHAR-2546] - Mocking dependencies should ideally be in the maven parent instead of individual modules

### New Feature
* [APEXMALHAR-2278] - Implement Kudu Output Operator for non-transactional streams
* [APEXMALHAR-2453] - Add sort Accumulation for Windowed operator
* [APEXMALHAR-2455] - Create example for Kafka 0.9 API exactly-once output operator
* [APEXMALHAR-2472] - Implement Kudu Input Operator
* [APEXMALHAR-2547] - Add ride data processing example to Apex Library

### Task
* [APEXMALHAR-2426] - Add user document for RegexParser operator
* [APEXMALHAR-2431] - Create Kinesis Input operator which emits byte array as tuple
* [APEXMALHAR-2459] - KafkaInputoperator using 0.10.* Kafka consumer API
* [APEXMALHAR-2463] - FTP Input Operator Example app and documentation
* [APEXMALHAR-2471] - Upgrade apex-core dependency to 3.6.0
* [APEXMALHAR-2479] - Create example for RegexParser operator
* [APEXMALHAR-2484] - BlockWriter for writing the part files into the specified directory
* [APEXMALHAR-2541] - Fix travis-ci build


Version 3.7.0 - 2017-03-31
------------------------------------------------------------------------------------------------------------------------

### Sub-task
* [APEXMALHAR-2001] - Fix all the checkstyle violations in kafka module
* [APEXMALHAR-2221] - Fix checkstyle violations in benchmark module
* [APEXMALHAR-2298] - Kafka ExactlyOnce (0.9) output operator fixes
* [APEXMALHAR-2301] - Implement another TimeBucketAssigner to work with any time
* [APEXMALHAR-2345] - Purge the time buckets from managed state for all time buckets that fall beyond the lateness horizon
* [APEXMALHAR-2389] - Add User Documentation for Calcite Integration
* [APEXMALHAR-2407] - Time buckets are not purging after expiry
* [APEXMALHAR-2408] - Issues in correctness of get() for key search in ManagedTimeStateImpl
* [APEXMALHAR-2409] - Improve PojoInnerJoin accumulation to emit a POJO instead of Map
* [APEXMALHAR-2414] - Improve performance of PojoInnerJoin accum by using PojoUtils
* [APEXMALHAR-2415] - Enable PojoInnerJoin accum to allow multiple keys for join purpose
* [APEXMALHAR-2439] - In apex-malhar, renaming "demos" to "examples"
* [APEXMALHAR-2440] - Identify and group examples based on type
* [APEXMALHAR-2441] - Move Non-Ingestion Examples from [datatorrent/examples] to [apex-malhar/examples]

### Bug
* [APEXMALHAR-2303] - S3 Line By Line Module
* [APEXMALHAR-2306] - Tests should allow for additions to OperatorContext interface
* [APEXMALHAR-2316] - Cannot register tuple class in XmlParser Operator
* [APEXMALHAR-2330] - JdbcPOJOPollInputOperator fails with NullPointerException when PostgreSQL driver
* [APEXMALHAR-2343] - Count Accumulation should only increase one for each tuple
* [APEXMALHAR-2346] - DocumentBuilder.parse() should take InputSource as an argument instead of String
* [APEXMALHAR-2350] - The key and value stream should match with the bucket
* [APEXMALHAR-2357] - JdbcPojoOperatorApplicationTest failing intermittently 
* [APEXMALHAR-2368] - JDBCPollInput operator reads extra records when 1.5M records are added to a blank input table
* [APEXMALHAR-2371] - Importing 'Apache Apex Malhar Iteration Demo' throws error for 'property' tag in properties.xml
* [APEXMALHAR-2379] - AbstractFileInputOperator bug fixes for regex, negative values
* [APEXMALHAR-2399] - In PojoInnerJoin accumulation default constructor is directly throwing an exception which messes up in default serialization.
* [APEXMALHAR-2400] - In PojoInnerJoin accumulation same field names are emitted as single field 
* [APEXMALHAR-2406] - ManagedState incorrect results for get()
* [APEXMALHAR-2418] - Update the twitter library to 4.0.6 
* [APEXMALHAR-2419] - KafkaSinglePortExactlyOnceOutputOperator fails on recovery
* [APEXMALHAR-2422] - WindowDataManager not recovering as expected on HDFS
* [APEXMALHAR-2424] - NullPointerException in JDBCPojoPollInputOperator with additional columns
* [APEXMALHAR-2450] - UniqueCounter emits empty maps even when there is no input
* [APEXMALHAR-2454] - CsvParser documentation xml formatting and rendering issue.

### Dependency upgrade
* [APEXMALHAR-2398] - commons-beanutils upgrade

### Documentation
* [APEXMALHAR-2183] - Add user document for CsvFormatter operator
* [APEXMALHAR-2364] - Add user documentation for S3OutputModule
* [APEXMALHAR-2370] - Add user documenation for Xml Parser
* [APEXMALHAR-2390] - Operator list in doc is not sorted
* [APEXMALHAR-2391] - JDBC Poller Input Operator exist but not listed 
* [APEXMALHAR-2432] - javadoc for cassandra operator is improperly formatted
* [APEXMALHAR-2433] - Add readme for Windowing Benchmark

### Improvement
* [APEXMALHAR-2220] - Move the FunctionOperator to Malhar library
* [APEXMALHAR-2344] - Initialize the list of FieldInfo in JDBCPollInput operator from properties.xml
* [APEXMALHAR-2354] - Add support for heuristic watermarks in WindowedOperator
* [APEXMALHAR-2358] - Optimise GenericSerde to use specific serde to improve the performance
* [APEXMALHAR-2359] - Optimise fire trigger to avoid go through all data
* [APEXMALHAR-2365] - LogParser - Operator to parse byte array using log format and emit a POJO
* [APEXMALHAR-2372] - Change the order of checks of table name in populateColumnDataTypes
* [APEXMALHAR-2374] - Recursive support for AbstractFileInputOperator
* [APEXMALHAR-2376] - Add Common Log support in LogParser operator
* [APEXMALHAR-2377] - Move LogParser operator to org.apache.apex.malhar.contrib.parser
* [APEXMALHAR-2380] - Add MutablePair for Kinensis Operator for Recovery State
* [APEXMALHAR-2381] - Change FSWindowManager for performance issues in Kinesis Input Operator
* [APEXMALHAR-2394] - AbstractFileOutputOperator.rotate(...) does not check if file has already been rotated before computing next rotation
* [APEXMALHAR-2411] - Avoid isreplaystate variable, incorporate logic in activate() and replay() for Kinesis Input Operator
* [APEXMALHAR-2412] - Provide emitTuple overriding functionality for user in kinesis Input operator
* [APEXMALHAR-2413] - Improve PojoInnerJoin Accumulation
* [APEXMALHAR-2429] - Ambiguity in passing "key" parameter to Join accumulation
* [APEXMALHAR-2430] - Optimize Join accumulation by changing the data structure in accumulation method
* [APEXMALHAR-2445] - KafkaExactlyOnce should not write to WAL during recovery

### New Feature
* [APEXMALHAR-2022] - S3 Output Module for file copy
* [APEXMALHAR-2130] - Scalable windowed storage
* [APEXMALHAR-2218] - RegexParser- Operator to parse byte stream using Regex pattern and emit a POJO
* [APEXMALHAR-2259] - Create Fixed Length Parser Operator
* [APEXMALHAR-2369] - S3 output module for tuple based output
* [APEXMALHAR-2416] - Development of Redshift Output Module
* [APEXMALHAR-2417] - Add PojoOuterJoin (left, right and full) accumulation
* [APEXMALHAR-2428] - CompositeAccumulation for windowed operator

Version 3.6.0 - 2016-11-30
------------------------------------------------------------------------------------------------------------------------

### Sub-task
* [APEXMALHAR-2244] - Optimize WindowedStorage and Spillable data structures for time series
* [APEXMALHAR-2248] - Create SpillableSet and SpillableSetMultimap interfaces and implementation

### Bug
* [APEXMALHAR-1852] - File Splitter Test Failing
* [APEXMALHAR-2176] - expressionFunctions for FilterOperator throws IndexOutOfBounds
* [APEXMALHAR-2207] - JsonFormatterTest application test should check for presence of expected results
* [APEXMALHAR-2217] - Remove some redundant code in WindowedStorage and WindowedKeyedStorage
* [APEXMALHAR-2224] - GenericFileOutputOperator rotateCall fails if no data is written to the file
* [APEXMALHAR-2226] - Not suppported Exception in AbstractFileOutput Operator.
* [APEXMALHAR-2227] - Error while connecting with Kafka using Apache Apex
* [APEXMALHAR-2236] - Potential NullPointerException in JdbcStore
* [APEXMALHAR-2245] - WindowBoundedMapCache.remove not working when key not in cache
* [APEXMALHAR-2246] - Key of SpillableByteArrayListMultimapImpl not comparable
* [APEXMALHAR-2256] - POJOInnerJoinOperator should use getDeclaredField of java reflection
* [APEXMALHAR-2258] - JavaExpressionParser does not cast type correctly when expression is binary
* [APEXMALHAR-2263] - Offsets in AbstractFileInputOperator should be long rather than int
* [APEXMALHAR-2265] - Add entries to mkdocs.yml for recently added operator docs
* [APEXMALHAR-2272] - sequentialFileRead property on FSInputModule not functioning as expected
* [APEXMALHAR-2273] - Retraction trigger is fired incorrectly when fireOnlyUpdatedPanes is true
* [APEXMALHAR-2276] - ManagedState: value of a key does not get over-written in the same time bucket
* [APEXMALHAR-2281] - ManagedState: race condition with put & asyncGet
* [APEXMALHAR-2290] - JDBCPOJOInsertOutput Operator - Optimization to populate metadata from database
* [APEXMALHAR-2291] - Exactly-once processing not working correctly for JdbcPOJOInsertOutputOperator
* [APEXMALHAR-2299] - TimeBasedDedupOperator throws exception during time bucket assignment in certain edge cases
* [APEXMALHAR-2305] - Change implementation of session window to reflect what is described in streaming 102 blog
* [APEXMALHAR-2307] - Session windows are not deleted properly after merge or extend
* [APEXMALHAR-2309] - TimeBasedDedupOperator marks new tuples as duplicates if expired tuples exist
* [APEXMALHAR-2312] - NullPointerException in FileSplitterInput when file path is specified
* [APEXMALHAR-2314] - Improper functioning in partitioning of sequentialFileRead property of FSRecordReader 
* [APEXMALHAR-2315] - Ignore Join Test for because of issues in POJOInnerJoinOperator
* [APEXMALHAR-2317] - Change SpillableBenchmarkApp to adapt the change on Spillable Data Structure
* [APEXMALHAR-2325] - Same block id is emitting from FSInputModule
* [APEXMALHAR-2329] - ManagedState benchmark should not use constant bucket
* [APEXMALHAR-2333] - StateTracker#run throws NoSuchElementException
* [APEXMALHAR-2334] - Managed State benchmark: blocked committed window 
* [APEXMALHAR-2342] - Fix null pointer exception in AbstractFileOutputOperator setup
* [APEXMALHAR-2351] - Exception while fetching properties for Operators using JdbcStore 
* [APEXMALHAR-2353] - timeExpression should not be null for time based Dedup

### Documentation
* [APEXMALHAR-2166] - Add user documentation for Json Parser
* [APEXMALHAR-2167] - Add user documentation for Json Formatter
* [APEXMALHAR-2179] - Add documentation for JDBCPollInputOperator
* [APEXMALHAR-2184] - Add documentation for FileSystem Input Operator
* [APEXMALHAR-2219] - Add documentation for Deduper
* [APEXMALHAR-2232] - Add documentation for csv parser
* [APEXMALHAR-2242] - Add documentation for 0.9 version of Kafka Input Operator.
* [APEXMALHAR-2257] - Add documentation for Transform operator
* [APEXMALHAR-2264] - Add documentation for jmsInputOperator
* [APEXMALHAR-2282] - Document Windowed Operator and Accumulation

### Improvement
* [APEXMALHAR-2017] - Use pre checkpoint notification to optimize operator IO
* [APEXMALHAR-2139] - UniqueCounter changes
* [APEXMALHAR-2237] - Dynamic partitioning support for FSInputModule
* [APEXMALHAR-2267] - Remove the word "Byte" in the spillable data structures because it's implied
* [APEXMALHAR-2280] - Add InterfaceStability annotations to all windowed operator related packages
* [APEXMALHAR-2302] - Exposing more properties of FSSplitter and BlockReader operators to FSRecordReaderModule
* [APEXMALHAR-2320] - FSWindowDataManager.toSlice() can cause lots of garbage collection
* [APEXMALHAR-2327] - BucketsFileSystem.writeBucketData() call Slice.toByteArray() cause allocate unnecessary memory
* [APEXMALHAR-2340] - Initialize the list of JdbcFieldInfo in JdbcPOJOInsertOutput from properties.xml

### New Feature
* [APEXMALHAR-1818] - Integrate Calcite to support SQL
* [APEXMALHAR-2152] - Enricher - Add fixed length file format support to FSLoader
* [APEXMALHAR-2181] - Non-Transactional Prepared Statement Based Cassandra Upsert (Update + Insert ) output Operator
* [APEXMALHAR-2209] - Add inner join example application to examples repository
* [APEXMALHAR-2229] - Add support for peek on com.datatorrent.lib.fileaccess.FileAccess.FileReader
* [APEXMALHAR-2247] - Add iteration feature in SpillableArrayListImpl and generalize SerdeListSlice to SerdeCollectionSlice
* [APEXMALHAR-2304] - Apex SQL: Add examples for SQL in Apex in demos folder

### Task
* [APEXMALHAR-2143] - Evaluate and retire lib/math, lib/algo, and lib/streamquery operators
* [APEXMALHAR-2190] - Use reusable buffer to serial spillable data structure
* [APEXMALHAR-2201] - Suppress console output in Stream API tests
* [APEXMALHAR-2225] - Upgrade checkstyle rules to 1.1.0 and fix trailing whitespace
* [APEXMALHAR-2240] - Implement Windowed Join Operator
* [APEXMALHAR-2338] - Couple of links in fsInputOperator.md have a stray # which prevents proper display

Version 3.5.0 - 2016-08-31
------------------------------------------------------------------------------------------------------------------------

### Sub-task
* [APEXMALHAR-2047] - Create Factory Which Can Easily Create A Single Spillable Data Structure
* [APEXMALHAR-2048] - Create concrete implementation of ArrayListMultiMap using managed state.
* [APEXMALHAR-2070] - Create In Memory Implementation of ArrayList Multimap
* [APEXMALHAR-2202] - Move accumulations to correct package
* [APEXMALHAR-2208] - High-level API beam examples

### Bug
* [APEXMALHAR-998] - Compilation error while using UniqueValueCount operator.
* [APEXMALHAR-1988] - CassandraInputOperator fetches less number of records inconsistenly
* [APEXMALHAR-2103] - Scanner issues in FileSplitterInput class
* [APEXMALHAR-2104] - BytesFileOutputOperator Refactoring
* [APEXMALHAR-2112] - Contrib tests are failing because of inclusion of apache logger with geode dependency
* [APEXMALHAR-2113] - Dag fails validation due to @NotNull on getUpdateCommand() in JdbcPOJOOutputOperator
* [APEXMALHAR-2119] - Make DirectoryScanner in AbstractFileInputOperator inheritance friendly. 
* [APEXMALHAR-2120] - Fix bugs on KafkaInputOperatorTest and AbstractKafkaInputOperator
* [APEXMALHAR-2128] - Update twitter4j version to the one support twitter APIs
* [APEXMALHAR-2134] - Catch NullPointerException if some Kafka partition has no leader broker
* [APEXMALHAR-2135] - Upgrade Kafka 0.8 input operator to support 0.8.2 client
* [APEXMALHAR-2136] - Null pointer exception in AbstractManagedStateImpl
* [APEXMALHAR-2138] - Multiple declaration of org.mockito.mockito-all-1.8.5 in Malhar library pom 
* [APEXMALHAR-2140] - Move ActiveFieldInfo class to com.datatorrent.lib.util
* [APEXMALHAR-2158] - Duplication of data emitted when the Kafka Input Operator(0.8 version) redeploys
* [APEXMALHAR-2168] - The setter method for double field is not generated correctly in JdbcPOJOInputOperator.
* [APEXMALHAR-2169] - KafkaInputoperator: Remove the stuff related to Partition Based on throughput.
* [APEXMALHAR-2171] - In CacheStore maxCacheSize is not applied
* [APEXMALHAR-2174] - S3 File Reader reading more data than expected
* [APEXMALHAR-2195] - LineReaderContext gives incorrect results for files not ending with the newline
* [APEXMALHAR-2197] - TimeBasedPriorityQueue.removeLRU throws NoSuchElementException
* [APEXMALHAR-2199] - 0.8 kafka input operator doesn't support chroot zookeeper path (multitenant kafka support)

### Documentation
* [APEXMALHAR-2153] - Add user documentation for Enricher

### Improvement
* [APEXMALHAR-1953] - Add generic (insert, update, delete) support to JDBC Output Operator
* [APEXMALHAR-1957] - Improve HBasePOJOInputOperator with support for threaded read
* [APEXMALHAR-1966] - Cassandra output operator improvements
* [APEXMALHAR-2028] - Add System.err to ConsoleOutputOperator 
* [APEXMALHAR-2045] - Bandwidth control feature
* [APEXMALHAR-2063] - Integrate WAL to FS WindowDataManager
* [APEXMALHAR-2069] - FileSplitterInput and TimeBasedDirectoryScanner - move operational fields initialization from constructor to setup
* [APEXMALHAR-2075] - Support fields of type Date,Time and Timestamp in Pojo Class for JdbcPOJOInputOperator 
* [APEXMALHAR-2087] - Hive output module
* [APEXMALHAR-2096] - Add blockThreshold parameter to FSInputModule
* [APEXMALHAR-2105] - Enhance CSV Formatter to take in schema similar to Csv Parser
* [APEXMALHAR-2111] - Projection Operator config params shall use List instead of comma-separated field names
* [APEXMALHAR-2121] - KafkaInputOperator emitTuple method should be able to emit more than just message
* [APEXMALHAR-2148] - Reduce the noise of kafka input operator
* [APEXMALHAR-2154] - Update kafka 0.9 input operator to use new CheckpointNotificationListener
* [APEXMALHAR-2156] - JMS Input operator enhancements
* [APEXMALHAR-2157] - Improvements in JSON Formatter
* [APEXMALHAR-2172] - Update JDBC poll input operator to fix issues
* [APEXMALHAR-2180] - KafkaInput Operator partitions has to be unchanged in case of dynamic scaling of ONE_TO_MANY strategy.
* [APEXMALHAR-2185] - Add a Deduper implementation for Bounded data

### New Feature
* [APEXMALHAR-1701] - Deduper backed by Managed State
* [APEXMALHAR-2019] - S3 Input Module
* [APEXMALHAR-2026] - Spill-able Datastructures
* [APEXMALHAR-2066] - JDBC poller input operator
* [APEXMALHAR-2082] - Data Filter Operator 
* [APEXMALHAR-2085] - Implement Windowed Operators
* [APEXMALHAR-2100] - Inner Join Operator using Spillable Datastructures
* [APEXMALHAR-2116] - File Record reader module
* [APEXMALHAR-2142] - High-level API window support
* [APEXMALHAR-2151] - Enricher - Add delimited file format support to FSLoader

### Task
* [APEXMALHAR-2129] - ManagedState: Disable purging based on system time
* [APEXMALHAR-2200] - Enable checkstyle for demos

### Test
* [APEXMALHAR-2161] - Add tests for AbstractThroughputFileInputOperator

Version 3.4.0 - 2016-05-24
------------------------------------------------------------------------------------------------------------------------

### Sub-task
* [APEXMALHAR-2006] - Stream API Design
* [APEXMALHAR-2046] - Introduce Spill-able data-structure interfaces
* [APEXMALHAR-2050] - Move spillable package under state.
* [APEXMALHAR-2051] - Remove redundant StorageAgent interface Malhar library 
* [APEXMALHAR-2064] - Move WindowDataManager to org.apache.apex.malhar.lib.wal
* [APEXMALHAR-2065] - Add getWindows() method to WindowDataManager
* [APEXMALHAR-2095] - Fix checkstyle violations of library module in Apex Malhar

### Bug
* [APEXMALHAR-1970] - ArrayOutOfBoundary error in One_To_Many Partitioner for 0.9 kafka input operator
* [APEXMALHAR-1973] - InitialOffset bug and duplication caused by offset checkpoint
* [APEXMALHAR-1984] - Operators that use Kryo directly would throw exception in local mode
* [APEXMALHAR-1985] - Cassandra Input Oeprator: startRow set incorrectly
* [APEXMALHAR-1990] - Occasional concurrent modification exceptions from IdempotentStorageManager
* [APEXMALHAR-1993] - Committed offsets are not present in offset manager storage for kafka input operator
* [APEXMALHAR-1994] - Operator partitions are reporting offsets for kafka partitions they don't subscribe to
* [APEXMALHAR-1998] - Kafka unit test memory requirement breaks Travis CI build
* [APEXMALHAR-2003] - NPE in FileSplitterInput
* [APEXMALHAR-2004] - TimeBasedDirectoryScanner keep reading same file
* [APEXMALHAR-2036] - FS operator tests leave stray test files under target
* [APEXMALHAR-2042] - Managed State - unexpected null value
* [APEXMALHAR-2052] - Enable checkstyle in parent POM
* [APEXMALHAR-2060] - Add an entry for org.apache.apex in the log4j.properties 
* [APEXMALHAR-2072] - Cleanup properties of Transform Operator
* [APEXMALHAR-2073] - Intermittent test failure: ManagedStateImplTest.testFreeWindowTransferRaceCondition
* [APEXMALHAR-2078] - Potential thread issue in FileSplitterInput class
* [APEXMALHAR-2079] - FileOutputOperator expireStreamAfterAccessMillis field typo
* [APEXMALHAR-2080] - File expiration time is set too low by default in AbstractFileOutputOperator.
* [APEXMALHAR-2081] - Remove FSFileSplitter, BlockReader, HDFSFileSplitter, HDFSInputModule
* [APEXMALHAR-2088] - Exception while fetching properties for Operators using JdbcStore 
* [APEXMALHAR-2097] - BytesFileOutputOperator class should be marked as public

### Improvement
* [APEXMALHAR-1873] - Create a fault-tolerant/scalable cache component backed by a persistent store
* [APEXMALHAR-1948] - CassandraStore Should Allow You To Specify Protocol Version.
* [APEXMALHAR-1961] - Enhancing existing CSV Parser
* [APEXMALHAR-1962] - Enhancing existing JSON Parser
* [APEXMALHAR-1980] - Add metrics to Cassandra Input operator
* [APEXMALHAR-1983] - Support special chars in topics setting for new Kafka Input Operator
* [APEXMALHAR-1991] - Move Dimensions Computation Classes to org.apache.apex.malhar package and Mark evolving
* [APEXMALHAR-2018] - HDFS File Input Module: Move generic code to abstract parent class.
* [APEXMALHAR-2025] - Move FileLineInputOperator out of AbstractFileInputOperator
* [APEXMALHAR-2031] - Allow Window Data Manager to store data in a user specified directory
* [APEXMALHAR-2043] - Update checkstyle plugin declaration to use apex-codestyle-config artifact
* [APEXMALHAR-2056] - Move Serde Interface Under utils and add methods which don't take mutable int
* [APEXMALHAR-2077] - SingleFileOutputOperator should append partitionId to file name

### New Feature
* [APEXMALHAR-1897] - Large operator state management
* [APEXMALHAR-1919] - Move Dimensional Schema To Malhar
* [APEXMALHAR-1920] - Add dimensional JDBC Output Operator
* [APEXMALHAR-1936] - Apache Nifi Connector
* [APEXMALHAR-1938] - Operator checkpointing in distributed in-memory store
* [APEXMALHAR-1942] - Apex Operator for Apache Geode.
* [APEXMALHAR-1972] - Create Expression Evaluator Support quasi-Java Expression Language
* [APEXMALHAR-2010] - Transform operator
* [APEXMALHAR-2011] - POJO to Avro record converter
* [APEXMALHAR-2012] - Avro Record to POJO converter
* [APEXMALHAR-2014] - ParquetReader operator
* [APEXMALHAR-2015] - Projection Operator
* [APEXMALHAR-2023] - Enrichment Operator

### Task
* [APEXMALHAR-1859] - Integrate checkstyle with Malhar
* [APEXMALHAR-1968] - Update NOTICE copyright year
* [APEXMALHAR-1969] - Add idempotency support to 0.9 KafkaInputOperator
* [APEXMALHAR-1975] - Add group id information to all apex malhar app package
* [APEXMALHAR-1986] - Change semantic version check to use 3.3 release
* [APEXMALHAR-2009] - concrete operator for writing to HDFS file
* [APEXMALHAR-2013] - HDFS output module for file copy
* [APEXMALHAR-2054] - Make the Query Operator in the App Data Pi Demo embedded in the Snapshot Server
* [APEXMALHAR-2055] - Add Dimension TOPN support
* [APEXMALHAR-2058] - Add simple byte[] to byte[] Serde implementation
* [APEXMALHAR-2067] - Make necessary changes in Malhar for Apex Core 3.4.0
* [APEXMALHAR-2093] - Remove usages of Idempotent Storage Manager

Version 3.3.1-incubating - 2016-02-27
------------------------------------------------------------------------------------------------------------------------

### Bug
* [APEXMALHAR-1970] - ArrayOutOfBoundary error in One_To_Many Partitioner for 0.9 kafka input operator
* [APEXMALHAR-1973] - InitialOffset bug and duplication caused by offset checkpoint
* [APEXMALHAR-1984] - Operators that use Kryo directly would throw exception in local mode
* [APEXMALHAR-1990] - Occasional concurrent modification exceptions from IdempotentStorageManager
* [APEXMALHAR-1993] - Committed offsets are not present in offset manager storage for kafka input operator
* [APEXMALHAR-1994] - Operator partitions are reporting offsets for kafka partitions they don't subscribe to
* [APEXMALHAR-1998] - Kafka unit test memory requirement breaks Travis CI build
* [APEXMALHAR-2003] - NPE in FileSplitterInput

### Improvement
* [APEXMALHAR-1983] - Support special chars in topics setting for new Kafka Input Operator

### Task
* [APEXMALHAR-1968] - Update NOTICE copyright year
* [APEXMALHAR-1986] - Change semantic version check to use 3.3 release

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

