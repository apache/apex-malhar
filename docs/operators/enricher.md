POJO Enricher
=============

## Operator Objective
This operator receives an POJO ([Plain Old Java Object](https://en.wikipedia.org/wiki/Plain_Old_Java_Object)) as an incoming tuple and uses an external source to enrich the data in 
the incoming tuple and finally emits the enriched data as a new enriched POJO.

POJOEnricher supports enrichment from following external sources:

1. **JSON File Based** - Reads the file in memory having content stored in JSON format and use that to enrich the data. This can be done using FSLoader implementation.
2. **JDBC Based** - Any JDBC store can act as an external entity to which enricher can request data for enriching incoming tuples. This can be done using JDBCLoader implementation.

POJO Enricher does not hold any state and is **idempotent**, **fault-tolerant** and **statically/dynamically partitionable**.

## Operator Usecase
1. Bank ***transaction records*** usually contains customerId. For further analysis of transaction one wants the customer name and other customer related information. 
Such information is present in another database. One could enrich the transaction's record with customer information using POJOEnricher.
2. ***Call Data Record (CDR)*** contains only mobile/telephone numbers of the customer. Customer information is missing in CDR. POJO Enricher can be used to enrich 
CDR with customer data for further analysis.

## Operator Information
1. Operator location: ***malhar-contrib***
2. Available since: ***3.4.0***
3. Operator state: ***Evolving***
3. Java Packages:
    * Operator: ***[com.datatorrent.contrib.enrich.POJOEnricher](https://www.datatorrent.com/docs/apidocs/com/datatorrent/contrib/enrich/POJOEnricher.html)***
    * FSLoader: ***[com.datatorrent.contrib.enrich.FSLoader](https://www.datatorrent.com/docs/apidocs/com/datatorrent/contrib/enrich/FSLoader.html)***
    * JDBCLoader: ***[com.datatorrent.contrib.enrich.JDBCLoader](https://www.datatorrent.com/docs/apidocs/com/datatorrent/contrib/enrich/JDBCLoader.html)***

## Properties, Attributes and Ports
### <a name="props"></a>Properties of POJOEnricher
| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *includeFields* | List of fields from database that needs to be added to output POJO. | List<String\> | Yes | N/A |
| *lookupFields* | List of fields from input POJO which will form a *unique composite* key for querying to store | List<String\> | Yes | N/A |
| *store* | Backend Store from which data should be queried for enrichment | [BackendStore](#backendStore) | Yes | N/A |
| *cacheExpirationInterval* | Cache entry expiry in ms. After this time, the lookup to store will be done again for given key | int | No | 1 * 60 * 60 * 1000 (1 hour) |
| *cacheCleanupInterval* | Interval in ms after which cache will be removed for any stale entries. | int | No | 1 * 60 * 60 * 1000 (1 hour) |
| *cacheSize* | Number of entry in cache after which eviction will start on each addition based on LRU | int | No | 1000 |

#### <a name="backendStore"></a>Properties of FSLoader (BackendStore)
| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *fileName* | Path of the file, the data from which will be used for enrichment. See [here](#JSONFileFormat) for JSON File format. | String | Yes | N/A |


#### Properties of JDBCLoader (BackendStore)
| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *databaseUrl* | Connection string for connecting to JDBC | String | Yes | N/A |
| *databaseDriver* | JDBC Driver class for connection to JDBC Store. This driver should be there in classpath | String | Yes | N/A |
| *tableName* | Name of the table from which data needs to be retrieved | String | Yes | N/A |
| *connectionProperties* | Command seperated list of advanced connection properties that need to be passed to JDBC Driver. For eg. *prop1:val1,prop2:val2* | String | No | null |
| *queryStmt* | Select statement which will be used to query the data. This is optional parameter in case of advanced query. | String | No | null |



### Platform Attributes that influences operator behavior
| **Attribute** | **Description** | **Type** | **Mandatory** |
| -------- | ----------- | ---- | ------------------ |
| *input.TUPLE_CLASS* | TUPLE_CLASS attribute on input port which tells operator the class of POJO which will be incoming | Class or FQCN| Yes |
| *output.TUPLE_CLASS* | TUPLE_CLASS attribute on output port which tells operator the class of POJO which need to be emitted | Class or FQCN | Yes |


### Ports
| **Port** | **Description** | **Type** | **Mandatory** |
| -------- | ----------- | ---- | ------------------ |
| *input* | Tuple which needs to be enriched are received on this port | Object (POJO) | Yes |
| *output* | Tuples that are enriched from external source are emitted from on this port | Object (POJO) | No |

## Limitations
Current POJOEnricher contains following limitation:

1. FSLoader loads the file content in memory. Though it loads only the composite key and composite value in memory, a very large amount of data would bloat the memory and make the operator go OOM. In case the filesize is large, allocate sufficient memory to the POJOEnricher.
2. Incoming POJO should be a subset of outgoing POJO.
3. [includeFields](#props) property should contains fields having same name in database column as well as outgoing POJO. For eg. If name of the database column is "customerName", then outgoing POJO should contains a field with the same name and same should be added to includeFields.
4. [lookupFields](#props) property should contains fields having same name in database column as well as incoming POJO. For eg. If name of the database column is "customerId", then incoming POJO should contains a field with the same name and same should be added to lookupFields.

## Example
Example for POJOEnricher can be found at: [https://github.com/DataTorrent/examples/tree/master/tutorials/enricher](https://github.com/DataTorrent/examples/tree/master/tutorials/enricher)

## Advanced

### <a name="JSONFileFormat"></a> File format for JSON based FSLoader
FSLoader expects file to be in specific format:

1. Each line makes on record which becomes part of the store
2. Each line is a valid JSON Object where *key* is name of the field name and *value* is the field value.

Example for the format look like following:
```json
{"circleId":0, "circleName":"A"}
{"circleId":1, "circleName":"B"}
{"circleId":2, "circleName":"C"}
{"circleId":3, "circleName":"D"}
{"circleId":4, "circleName":"E"}
{"circleId":5, "circleName":"F"}
{"circleId":6, "circleName":"G"}
{"circleId":7, "circleName":"H"}
{"circleId":8, "circleName":"I"}
{"circleId":9, "circleName":"J"}
```

### Caching mechanism in POJOEnricher
POJOEnricher contains an cache which makes the lookup for keys more efficient. This is specially useful when data in external store is not changing much. 
However, one should carefully tune the [cacheExpirationInterval](#props) property for desirable results.

On every incoming tuple, POJOEnricher first queries the cache. If the cache contains desired record and is within expiration interval, then it uses that to
enrich the tuple, otherwise does a lookup to configured store and the return value is used to enrich the tuple. The return value is then cached for composite key and composite value.

POJOEnricher only caches the required fields for enrichment mechanism and not all fields returned by external store. This ensures optimal use of memory.


### Partitioning of POJOEnricher
Being stateless operator, POJOEnricher will ensure built-in partitioners present in Malhar library can be directly simply by setting few properties as follows:

#### Stateless partioning of POJOEnricher
Stateless partitioning will ensure that POJOEnricher will will be partitioned right at the starting of the application and will remain partitioned throughout the lifetime of the DAG.
POJOEnricher can be stateless partitioned by adding following lines to properties.xml:

```xml
  <property>
    <name>dt.operator.{OperatorName}.attr.PARTITIONER</name>
    <value>com.datatorrent.common.partitioner.StatelessPartitioner:2</value>
  </property>
```

where {OperatorName} is the name of the POJOEnricher operator.
Above lines will partition POJOEnricher statically 2 times. Above value can be changed accordingly to change the number of static partitions.


#### Dynamic Partitioning of POJOEnricher
Dynamic partitioning is a feature of Apex platform which changes the partition of the operator based on certain condition.
POJOEnricher can be dynamically partitioned using 2 out-of-the-box partitioners:

##### Throughput based
Following code can be added to populateDAG method of application to dynamically partitioning POJOEnricher:
```java
    StatelessThroughputBasedPartitioner<POJOEnricher> partitioner = new StatelessThroughputBasedPartitioner<>();
    partitioner.setCooldownMillis(conf.getLong(COOL_DOWN_MILLIS, 10000));
    partitioner.setMaximumEvents(conf.getLong(MAX_THROUGHPUT, 30000));
    partitioner.setMinimumEvents(conf.getLong(MIN_THROUGHPUT, 10000));
    dag.setAttribute(pojoEnricherObj, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{partitioner}));
    dag.setAttribute(pojoEnricherObj, OperatorContext.PARTITIONER, partitioner);
```

Above code will dynamically partition POJOEnricher when the throughput changes.
If the overall throughput of POJOEnricher goes beyond 30000 or less than 10000, the platform will repartition POJOEnricher 
to balance throughput of a single partition to be between 10000 and 30000.
CooldownMillis of 10000 will be used as the threshold time for which the throughout change is observed.

##### Latency based
Following code can be added to populateDAG method of application to dynamically partitioning POJOEnricher:
```java
    StatelessLatencyBasedPartitioner<POJOEnricher> partitioner = new StatelessLatencyBasedPartitioner<>();
    partitioner.setCooldownMillis(conf.getLong(COOL_DOWN_MILLIS, 10000));
    partitioner.setMaximumLatency(conf.getLong(MAX_THROUGHPUT, 10));
    partitioner.setMinimumLatency(conf.getLong(MIN_THROUGHPUT, 3));
    dag.setAttribute(pojoEnricherObj, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{partitioner}));
    dag.setAttribute(pojoEnricherObj, OperatorContext.PARTITIONER, partitioner);
```

Above code will dynamically partition POJOEnricher when the overall latency of POJOEnricher changes.
If the overall latency of POJOEnricher goes beyond 10 ms or less than 3 ms, the platform will repartition POJOEnricher 
to balance latency of a single partition to be between 3 ms and 10 ms.
CooldownMillis of 10000 will be used as the threshold time for which the latency change is observed.



