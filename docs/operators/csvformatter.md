CsvFormatter
============

## Operator Objective
This operator receives a POJO ([Plain Old Java Object](https://en.wikipedia.org/wiki/Plain_Old_Java_Object)) as an incoming tuple, converts the data in 
the incoming POJO to a custom delimited string and emits the delimited string.

CsvFormatter supports schema definition as a JSON string. 

CsvFormatter does not hold any state and is **idempotent**, **fault-tolerant** and **statically/dynamically partitionable**.

## Operator Information
1. Operator location: ***malhar-contrib***
2. Available since: ***3.2.0***
3. Operator state: ***Evolving***
3. Java Packages:
    * Operator: ***[com.datatorrent.contrib.formatter.CsvFormatter](https://www.datatorrent.com/docs/apidocs/com/datatorrent/contrib/formatter/CsvFormatter.html)***
    
## Properties, Attributes and Ports
### <a name="props"></a>Properties of POJOEnricher
| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *schema* | Contents of the schema.Schema is specified in a json format. | String | Yes | N/A |


### Platform Attributes that influences operator behavior
| **Attribute** | **Description** | **Type** | **Mandatory** |
| -------- | ----------- | ---- | ------------------ |
| *in.TUPLE_CLASS* | TUPLE_CLASS attribute on input port which tells operator the class of POJO which will be incoming | Class or FQCN| Yes |


### Ports
| **Port** | **Description** | **Type** | **Mandatory** |
| -------- | ----------- | ---- | ------------------ |
| *in* | Tuples which need to be formatted are received on this port | Object (POJO) | Yes |
| *out* | Tuples that are formatted are emitted from this port | String | No |
| *err* | Tuples that could not be converted are emitted on this port | Object | No |

## Limitations
Current CsvFormatter contain following limitations:

1. The field names in schema and the pojo field names should match.For eg. if name of the schema field is "customerName", then POJO should contain a field with the same name. 
2. Field wise validation/formatting is not yet supported.
3. The fields will be written to the file in the same order as specified in schema.json

## Example
Example for CsvFormatter can be found at: [https://github.com/DataTorrent/examples/tree/master/tutorials/csvformatter](https://github.com/DataTorrent/examples/tree/master/tutorials/csvformatter)

## Advanced

### <a name="JSONFileFormat"></a> Schema format for CsvFormatter
CsvFormatter expects schema to be a String in JSON format:


Example for format of schema:
```json
{
  "separator": ",",
  "quoteChar": "\"",
  "lineDelimiter": "\n",
  "fields": [
    {
      "name": "campaignId",
      "type": "Integer"
    },
    {
      "name": "startDate",
      "type": "Date",
      "constraints": {
        "format": "yyyy-MM-dd"
      }
    }
    ]
}
```


### Partitioning of CsvFormatter
Being stateless operator, CsvFormatter will ensure built-in partitioners present in Malhar library can be directly used by setting properties as follows:

#### Stateless partioning of CsvFormatter
Stateless partitioning will ensure that CsvFormatter will be partitioned right at the start of the application and will remain partitioned throughout the lifetime of the DAG.
CsvFormatter can be stateless partitioned by adding following lines to properties.xml:

```xml
  <property>
    <name>dt.operator.{OperatorName}.attr.PARTITIONER</name>
    <value>com.datatorrent.common.partitioner.StatelessPartitioner:2</value>
  </property>
```

where {OperatorName} is the name of the CsvFormatter operator.
Above lines will partition CsvFormatter statically 2 times. Above value can be changed accordingly to change the number of static partitions.


#### Dynamic Partitioning of CsvFormatter
Dynamic partitioning is a feature of Apex platform which changes the partition of the operator based on certain conditions.
CsvFormatter can be dynamically partitioned using below out-of-the-box partitioner:

##### Throughput based
Following code can be added to populateDAG method of application to dynamically partition CsvFormatter:
```java
    StatelessThroughputBasedPartitioner<CsvFormatter> partitioner = new StatelessThroughputBasedPartitioner<>();
    partitioner.setCooldownMillis(conf.getLong(COOL_DOWN_MILLIS, 10000));
    partitioner.setMaximumEvents(conf.getLong(MAX_THROUGHPUT, 30000));
    partitioner.setMinimumEvents(conf.getLong(MIN_THROUGHPUT, 10000));
    dag.setAttribute(csvFormatter, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{partitioner}));
    dag.setAttribute(csvFormatter, OperatorContext.PARTITIONER, partitioner);
```

Above code will dynamically partition CsvFormatter when throughput changes.
If overall throughput of CsvFormatter goes beyond 30000 or less than 10000, the platform will repartition CsvFormatter 
to balance throughput of a single partition to be between 10000 and 30000.
CooldownMillis of 10000 will be used as threshold time for which  throughput change is observed.

