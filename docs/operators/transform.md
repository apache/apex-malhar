Transform - Operator Documentation
==================================

### About Transform operator
----------------------------

Transform means mapping of field expression from input to output or conversion of fields from one type to another.
This operator is stateless. This operator receives objects on its input port; for each such input object, it creates a new output object whose fields are computed as expressions involving fields of the input object. 
The types of the input and output objects are configurable as are the expressions used to compute the output fields. 

The operator class is `TransformOperator` located in the package `com.datatorrent.lib.transform`.
Please refer to [github URL](https://github.com/apache/apex-malhar/blob/master/library/src/main/java/com/datatorrent/lib/transform/TransformOperator.java) for `TransformOperator`.


### Use Case
------------

Consider the data that needs to be transformed as per output schema.

Consider input objects with these fields:

| Name        | Type           |
|-------------|----------------|
| FirstName   | String         |
| LastName    | String         |
| Phone       | String         |
| DateOfBirth | java.util.Date |
| Address     | String         |
 
 and output objects with fields: 
 
| Name    | Type    |
|---------|---------|
| Name    | String  |
| Phone   | String  |
| Age     | Integer |
| Address | String  |
 
Suppose `Name` is a concatenation of `FirstName` and `LastName` and 
        `Age` is computed by subtracting the `DateOfBirth` from the current year.

These simple computations can be expressed as Java expressions where the input object is
represented by $ and provided as configuration parameters as follows:

```
Name => {$.FirstName}.concat(\" \").concat({$.LastName})
Age => (new java.util.Date()).getYear() - {$.dateOfBirth}.getYear()
```

### Configuration Parameters
-----------------------------

-   ***expressionMap*** -   Map<String, String>
    -   Mandatory Parameter
    -   Specifies the map between the output field (key) and the expression used to compute it (value) using fields of the input Java object.

-   ***expressionFunctions*** -   List<String>
    -   List of imported classes or methods should be made available to expression to use. It overrides the default list.
    -   Default Value = {java.lang.Math.*, org.apache.commons.lang3.StringUtils.*, org.apache.commons.lang3.StringEscapeUtils.*, org.apache.commons.lang3.time.DurationFormatUtils.*, org.apache.commons.lang3.time.DateFormatUtils.*}

        
-   ***copyMatchingFields*** -   boolean
    -   Specifies whether matching fields should be copied; here matching means the name and type of an input field is the same as the name and type of an output field. 
        If the matching field appears in `expressionMap` then it ignores copy to output object.
    -   Default Value = true.
    
### Configuration Example
-------------------------

Consider input object with fields:

| Name      | Type                   |
|-----------|------------------------|
| FirstName | String                 |
| LastName  | String                 |
| StartDate | org.joda.time.DateTime |

and output objects with fields:

| Name       | Type    |
|------------|---------|
| Name       | String  |
| isLeapYear | Boolean |

Note: `org.joda.time.DateTime` class is not present in the default list. So, we need to add this library to `expressionFunctions` as below in populateDAG method:
```java
TransformOperator operator = dag.addOperator("transform", new TransformOperator());
operator.setExpressionFunctions(Arrays.asList("org.joda.time.DateTime", org.apache.commons.lang3.StringUtils));
Map<String,String> expressionMap = new HashMap<>();
expressionMap.put(isLeapYear, {$.StartDate}.year().isLeap());
expressionMap.put(Name, org.apache.commons.lang3.StringUtils.joinWith(\" \", {$.FirstName},{$.LastName});
operator.setExpressionMap(expressionMap);
```

Above Properties also can be set in properties file as follows:

```xml
<property>
  <name>dt.operator.transform.expressionFunctions[0]</name>
  <value>org.joda.time.DateTime</value>
</property>     
<property>
  <name>dt.operator.transform.expressionFunctions[1]</name>
  <value>org.apache.commons.lang3.StringUtils</value>
</property>
<property>
  <name>dt.operator.transform.expressionMap(isLeapYear)</name>
  <value>{$.StartDate}.year().isLeap()</value>
</property>
<property>
  <name>dt.operator.transform.expressionMap(Name)</name>
  <value>org.apache.commons.lang3.StringUtils.joinWith(\" \", {$.FirstName}, {$.LastName})</value>
</property>
```
        
### Ports
----------

-   ***input*** -   Port for input tuples.
    -   Mandatory input port
    
-   ***output***    -   Port for transformed output tuples.
    -   Mandatory output port

### Attributes
---------------
-   ***Input port Attribute - input.TUPLE\_CLASS*** - Fully qualified class name and class should be Kryo serializable.
    -   Mandatory attribute
    -   Type of input tuple.

-   ***Output port Attribute - output.TUPLE\_CLASS*** - Fully qualified class name and class should be Kryo serializable.
    -   Mandatory attribute
    -   Type of output tuple.
    
### Application Example
------------------------

Please refer [Example](https://github.com/DataTorrent/examples/tree/master/tutorials/transform) for transform sample application.

### Partitioning
----------------
Being stateless, this operator can be partitioned using any of the built-in partitioners present in the Malhar library by setting a few properties as follows:

#### Stateless partitioning
Stateless partitioning will ensure that TransformOperator will be partitioned right at the starting of the application and will remain partitioned throughout the lifetime of the DAG.
TransformOperator can be stateless partitioned by adding following lines to properties.xml:

```xml
  <property>
    <name>dt.operator.{OperatorName}.attr.PARTITIONER</name>
    <value>com.datatorrent.common.partitioner.StatelessPartitioner:{N}/value>
  </property>
```

where {OperatorName} is the name of the TransformOperator operator and
      {N} is the number of static partitions.
Above lines will partition TransformOperator statically {N} times. 

#### Dynamic Partitioning
Dynamic partitioning is a feature of Apex platform which changes the partition of the operator based on certain condition.
TransformOperator can be dynamically partitioned using the below two partitioners:

##### Throughput based
Following code can be added to populateDAG(DAG dag, Configuration conf) method of application to dynamically partitioning TransformOperator:
```java
StatelessThroughputBasedPartitioner<TransformOperator> partitioner = new StatelessThroughputBasedPartitioner<>();
partitioner.setCooldownMillis(10000);
partitioner.setMaximumEvents(30000);
partitioner.setMinimumEvents(10000);
dag.setAttribute(transform, OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{partitioner}));
dag.setAttribute(transform, OperatorContext.PARTITIONER, partitioner);
```

Above code will dynamically partition TransformOperator when the throughput changes.
If the overall throughput of TransformOperator goes beyond 30000 or less than 10000, the platform will repartition TransformOperator 
to balance throughput of a single partition to be between 10000 and 30000.
CooldownMillis of 10000 will be used as the threshold time for which the throughout change is observed.

Source code for this dynamic application can be found [here](https://github.com/DataTorrent/examples/blob/master/tutorials/transform/src/main/java/com/example/transform/DynamicTransformApplication.java).