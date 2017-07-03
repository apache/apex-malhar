JDBC Poller Input Operator
=============

## Operator Objective
This operator scans JDBC database table in parallel fashion. This operator is added to address common input operator problems like,

1. As discussed in [Development Best Practices](https://github.com/apache/apex-core/blob/master/docs/development_best_practices.md),
    the operator callbacks such as `beginWindow()`, `endWindow()`, `emitTuples()`, etc.
    (which are invoked by the main operator thread)
    are required to return quickly, well within the default streaming window duration of
    500ms. This requirement can be an issue when retrieving data from slow external systems
    such as databases or object stores: if the call takes too long, the platform will deem
    the operator blocked and restart it. Restarting will often run into the same issue
    causing an unbroken sequence of restarts.

2. When a large volume of data is available from a single store that allows reading from
   arbitrary locations (such as a file or a database table), reading the data sequentially
   can be throughput limiting: Having multiple readers read from non-overlapping sections
   of the store allows any downstream parallelism in the DAG to be exploited better to
   enhance throughput. For files, this approach is used by the file splitter and block
   reader operators in the Malhar library.

JDBC Poller Input operator addresses the first issue with an asynchronous worker thread which retrieves the data and adds it to an in-memory queue; the main operator thread dequeue tuples very quickly if data is available or simply returns if not. The second is addressed in a way that parallels the approach to files by having multiple partitions read records from non-overlapping areas of the table. Additional details of how this is done are described below.

#### Assumption
Assumption is that there is an ordered column using which range queries can be formed. That means database has a column or combination of columns which has unique constraint as well as every newly inserted record should have column value more than max value in that column, as we poll only appended records.

## Use cases
1. Ingest large database tables. An example application that copies database contents to HDFS is available [here](https://github.com/apache/apex-malhar/blob/master/examples/jdbc/src/main/java/org/apache/apex/examples/JdbcIngest/JdbcPollerApplication.java).

## How to Use?
The tuple type in the abstract class is a generic parameter. Concrete subclasses need to choose an appropriate class (such as String or an appropriate concrete java class, having no-argument constructor so that it can be serialized using Kryo). Also implement a couple of abstract methods: `getTuple(ResultSet)` to convert database rows to objects of concrete class and `emitTuple(T)` to emit the tuple.

In principle, no ports need be defined in the rare case that the operator simply writes tuples to some external sink or merely maintains aggregated statistics. But in most common scenarios, the tuples need to be sent to one or more downstream operators for additional processing such as parsing, enrichment or aggregation; in such cases, appropriate output ports are defined and the emitTuple(T) implementation dispatches tuples to the desired output ports.

Couple of concrete implementations are provided in Malhar:

1. [JdbcPOJOPollInputOperator](https://github.com/apache/apex-malhar/blob/master/library/src/main/java/com/datatorrent/lib/db/jdbc/JdbcPOJOPollInputOperator.java): It uses java Object for the generic parameter. This operator defines a single output port and processes each database table record one by one as a tuple object. You need to set the output port attribute TUPLE_CLASS to define your [POJO](https://en.wikipedia.org/wiki/Plain_Old_Java_Object) class name to define Object type. The record fetched from the database table will be parsed, using the `getTuple` method mentioned above, as an object of the configured class. Details are discussed below.

2. [JdbcPollInputOperator](https://github.com/apache/apex-malhar/blob/master/library/src/main/java/com/datatorrent/lib/db/jdbc/JdbcPollInputOperator.java): It uses String for the generic parameter. This operator defines a single port and processes each database table record one by one as String tuple. Details are discussed below.

## Partitioning of JDBC Poller
#### Static Partitioning
Only static partitioning is supported for JDBC Poller Input Operator. Configure parameter `partitionCount` to define the desired number of initial partitions (4 in this example).
**Note**: An additional partition will be created to poll newly added records, so the total number of partitions will always be 1 + partitionCount.

```xml
  <property>
    <name>apex.operator.{OperatorName}.prop.partitionCount</name>
    <value>4</value>
  </property>
```

where {OperatorName} is the name of the JDBC Poller operator.

This will create 5 operator instances in all. Four of these will fetch the data which is currently in the table. We call these static non-polling partitions. The partitions will be idle after they fetch the portion of the data. An additional partition will be created which will read any newly added data. We call such a partition as a polling partition, as it "polls" for newly added data. There will be only one polling partition.

#### Dynamic Partitioning
Not supported.

## Operator Information
1. Operator location: ***malhar-library***
2. Available since: ***3.5.0***
3. Operator state: ***Evolving***
4. Java Packages: ***[AbstractJdbcPollInputOperator](https://ci.apache.org/projects/apex-malhar/apex-malhar-javadoc-release-3.7/com/datatorrent/lib/db/jdbc/package-summary.html)***

JDBC Poller is **idempotent**, **fault-tolerant** and **statically partitionable**.

## AbstractJdbcPollInputOperator
This is the abstract implementation that serves as base class for polling messages from JDBC store. It can be extended to modify functionality or add new capabilities. This class doesn’t have any ports, so concrete subclasses will need to provide them if necessary.

![AbstractJdbcPollInputOperator.png](images/jdbcinput/operatorsClassDiagram.png)

###<a name="AbstractJdbcPollInputOperatorProps"></a>Properties of AbstractJdbcPollInputOperator
Several properties are available to configure the behavior of this operator and they are summarized in the table below.

| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *store* | JDBC Store for connection | [JDBCStore](#JDBCStore) | Yes | N/A |
| *tableName* | table name to be scanned | String | Yes | N/A |
| *columnsExpression* | Comma separated list of columns to select from the given table. | String | Yes | N/A |
| *key* | Primary key column name | String | Yes | N/A |
| *partitionCount* | Static partitions count | int | No | 1 |
| *whereCondition* | Where condition for JDBC query | String | No | N/A |
| *fetchSize* | Hint limiting the number of rows to fetch in a single call | int | No | 20000 |
| *pollInterval* | Interval in milliseconds to poll the database table | int | No | 10000 |
| *queueCapacity* | Capacity of queue which holds DB data before emiting | int | No | 4096 |
| *batchSize* | Maximum number of tuples to emit in a single call to the `emitTuples()` callback (see explanation above). | int | No | 2000 |

#### <a name="JDBCStore"></a>Properties of JDBC Store (BackendStore)
| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *databaseDriver* |JDBC Driver class for connection to JDBC Store. This driver should be there in class path | String | Yes | N/A |
| *databaseUrl* | Database url of the form jdbc:subprotocol:subname | String | Yes | N/A |
| *connectionProps* | Comma separated connection properties e.g. user:xyz,password:ijk | String | Yes | N/A |

Of these only `store` properties, `tableName`, `columnsExpression` and `key` are mandatory. Those properties can be set like this:

```xml
<property>
  <name>apex.operator.{OperatorName}.prop.tableName</name>
  <value>mytable</value>
</property>
<property>
  <name>apex.operator.{OperatorName}.prop.columnsExpression</name>
  <value>column1,column2,column4</value>
</property>
<property>
  <name>apex.operator.{OperatorName}.prop.key</name>
  <value>keycolumn</value>
</property>
<property>
  <name>apex.operator.{OperatorName}.prop.store.databaseDriver</name>
  <value>com.mysql.jdbc.Driver</value>
</property>
<property>
  <name>apex.operator.{OperatorName}.prop.store.databaseUrl</name>
  <value>jdbc:mysql://localhost:3306/mydb</value>
</property>
<property>
  <name>apex.operator.{OperatorName}.prop.store.connectionProps</name>
  <value>user:myuser,password:mypassword</value>
</property>
```

* If you need to filter the table records, set `whereCondition` which will be added to the generated SQL query.
* If you have a table with a very large number or rows to scan set `partitionsCount` to a higher number to increase read parallelism.
* The operator uses PreparedStatement, a precompiled SQL statement to fetch records from database table. You can set `fetchSize` as a hint to the database driver to restrict number of rows to fetch in one call. The remaining rows will be fetched in subsequent calls. Please note, some of the database drivers may not honor this hint. Please refer to database driver documentation to know recommended value.
* The platform invokes the `emitTuples()` callback multiple time in each streaming window; within a single such call, if a large number of tuples are emitted, there is some risk that they may overwhelm the downstream operators especially if they are performing some compute intensive operation. For such cases, output can be throttled by reducing the value of the `batchSize` property. Conversely, if the downstream operators can handle the load, increase the value to enhance throughput.
* If there is high rate of incoming records in your table and you want to process them as soon as they appear, use lower value of `pollInterval`; if they appear rarely or if some delay in processing new records is acceptable, increase it.
* After reading the records from the table they are held in memory for some time till they are emitted to next operator. The records are kept in a blocking queue. The capacity of this blocking queue can be changed using parameter `queueCapacity`. You can use larger size of queue when your reader thread is very fast and you want to read more data in memory to keep it ready for emission.

**Note**: Please set right store object instance to JDBC Poller Input Operator using your application code. It's recommended to use [JdbcStore](https://github.com/apache/apex-malhar/blob/master/library/src/main/java/com/datatorrent/lib/db/jdbc/JdbcStore.java) for this operator.

### Abstract Methods
`void emitTuple(T tuple)`: Abstract method that emits tuple extracted from JDBC store.

`T getTuple(ResultSet result)`: Abstract method to extract the tuple from the JDBC ResultSet object and convert it to the required type (T).

## Concrete Classes
### 1. JdbcPOJOPollInputOperator
This implementation converts JDBC store records to [POJO](https://en.wikipedia.org/wiki/Plain_Old_Java_Object) and emits POJO on output port.

#### <a name="JdbcPOJOPollInputOperatorProps"></a>Properties of JdbcPOJOPollInputOperator
This operator defines following additional properties beyond those defined in the [parent class](#AbstractJdbcPollInputOperatorProps).

| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *fieldInfos*| Maps columns to POJO field names.| List | Yes | N/A |

#### Platform Attributes that influence operator behavior
| **Attribute** | **Description** | **Type** | **Mandatory** |
| -------- | ----------- | ---- | ------------------ |
| *outputPort.TUPLE_CLASS* | TUPLE_CLASS attribute on output port which tells operator the class of POJO which need to be emitted | Class or FQCN (Fully Qualified Class Name) | Yes |

#### Ports
| **Port** | **Description** | **Type** | **Mandatory** |
| -------- | ----------- | ---- | ------------------ |
| *outputPort* | Tuples that are read from JDBC store are emitted from on this port | Object (POJO) | No |

### 2. JdbcPollInputOperator
This implementation converts JDBC store records to comma separated CSV records. This operator is normally used when you just want to copy the data from database to somewhere else and don't want to do much of processing.

#### <a name="props"></a>Properties of JdbcPollInputOperator
This operator defines no additional properties beyond those defined in the [parent class](#AbstractJdbcPollInputOperatorProps).

#### Ports
| **Port** | **Description** | **Type** | **Mandatory** |
| -------- | ----------- | ---- | ------------------ |
| *outputPort* | Tuples that are read from JDBC store are emitted on this port | String | No |

## Limitations
Out of order insertion/deletion won't be supported.

