File Input Operator
=============

## Operator Objective
This operator is designed to scan a directory for files, read and split file content into tuples
such as lines or a block of bytes, and finally emit them on output ports defined in concrete
subclasses for further processing by downstream operators.
It can be used with any filesystem supported by Hadoop like HDFS, S3, ftp, NFS etc.

## Overview
The operator is **idempotent**, **fault-tolerant** and **partitionable**.

Logic for directory scanning is encapsulated in the `DirectoryScanner` static inner class
which provides functions such as matching file names against a regular expression, tracking files
that have already been processed (so that they are not processed again), filtering files based
on the hashcode of the file names in the presence of partitioning so that each file is
processed by a unique partition. This class can be extended if necessary to provide
additional capabilities such as scanning multiple directories.

It tracks the current file offset as part of checkpoint state. It it fails and is restarted
by the platform, it will seek to the saved offset to avoid duplicate processing. Exactly once processing
for fault tolerance is handled using window data manager. For more details check the blog about [Fault-Tolerant File Processing](https://www.datatorrent.com/blog/fault-tolerant-file-processing/).
It supports both static and dynamic partitioning.

## Use Cases
This operator is suitable for use in an environment where small to medium sized files are
deposited in a specific directory on a regular basis. For very large files a better alternative
is the `FileSplitter` and `BlockReader` combination since they allow such files to be processed
by multiple partitions to achieve higher throughput. Additionally, files which are continually
modified by other processes are not suitable for processing with this operator since they may
yield unpredictable results.

## How to Use?
The tuple type in the abstract class is a generic parameter.
Concrete subclasses need to choose an appropriate class (such as `String` or `byte[]`) for the
generic parameter and also implement a couple of abstract methods: `readEntity()` to read
the next tuple from the currently open file and `emit()` to process the next tuple.

In principle, no ports need be defined in the rare case that the operator simply writes
tuples to some external sink or merely maintains aggregated statistics. But in most common
scenarios, the tuples need to be sent to one or more downstream operators for additional
processing such as parsing, enrichment or aggregation; in such cases, appropriate
output ports are defined and the `emit()` implementation dispatches tuples to the
desired output ports.

A simple concrete implementation is provided in Malhar: `LineByLineFileInputOperator`.
It uses `String` for the generic parameter, defines a single output port and processes each
line of the input file as a tuple. It is discussed further below.

## Partitioning
#### Static Partitioning
Configure parameter `partitionCount` to define the desired number of initial partitions
(4 in this example).

```xml
<property>
  <name>dt.operator.{OperatorName}.prop.partitionCount</name>
  <value>4</value>
</property>
```

where _{OperatorName}_ is the name of the input operator.

#### Dynamic Partitioning
Dynamic partitioning -- changing the number of partitions of one or more operators
in a running application -- can be achieved in multiple ways:
- Use the command line tool `apex` or the UI console to change the value of the
  `partitionCount` property of the running operator. This change is detected in
  `processStats()` (which is invoked periodically by the platform) where, if the
  current partition count (`currentPartitions`) and the desired partition count
  (`partitionCount`) differ, the `repartitionRequired` flag in the response is set.
  This causes the platform to invoke `definePartitions()` to create a new set of
  partitions with the desired count.
- Override `processStats()` and within it, based on the statistics in the
  incoming parameter or any other factors, define a new desired value of
  `partitionCount` and finally, if this value differs from the current partition
  count, set the `repartitionRequired` flag in the response.

The details of actually creating the new set of partitions can be customized by overriding
the `definePartitions()` method. There are a couple of things to keep in mind when doing this.
The first is that repartitioning needs some care when the operator has state (as is the
case here): Existing state from current operator partitions needs to redistributed to the
new partitions in a logically consistent way. The second is that some or all of the
current set of partitions, which is an input parameter to `definePartitions()`, can be
copied over to the new set; such partitions will continue running and will not be
restarted. Any existing partitions that are not present in the new set will be shutdown.
The current re-partitioning logic does not preserve any existing partitions, so upon
a repartition event, all existing partitions are shutdown and the new ones started.

## Operator Information
1. Operator location: ***malhar-library***
2. Available since: ***1.0.2***
3. Operator state: ***Stable***
3. Java Packages:
    * Operator: ***[com.datatorrent.lib.io.fs.AbstractFileInputOperator](https://www.datatorrent.com/docs/apidocs/com/datatorrent/lib/io/fs/AbstractFileInputOperator.html)***

### AbstractFileInputOperator
This is the abstract implementation that, as noted above, scans a single directory.
It can be extended to modify functionality or add new capabilities. For example, the
directory scanner can be overriden to monitor multiple directories. [This](https://github.com/DataTorrent/examples/tree/master/tutorials/fileIO-multiDir) example demonstrates how to do that.
As noted in the overview above, this class has no ports, so concrete subclasses will need to
provide them if necessary.

![AbstractFileInputOperator.png](images/fsInput/operatorsClassDiagram.png)

### <a name="AbstractFileInputOperatorProps"></a>Properties of AbstractFileInputOperator
Several properties are available to configure the behavior of this operator and they are
summarized in the table below. Of these, only `directory` is required: it specifies
the path of the monitored directory. It can be set like this:

```xml
<property>
  <name>dt.operator.{OperatorName}.prop.directory</name>
  <value>/tmp/fileInput</value>
</property>
```

 If new files appear with high frequency in this directory
and they need to be processed as soon as they appear, reduce the value of `scanIntervalMillis`;
if they appear rarely or if some delay in processing a new file is acceptable, increase it.
Obviously, smaller values will result in greater IO activity for the corresponding filesystem.

The platform invokes the `emitTuples()` callback multiple time in each streaming window; within
a single such call, if a large number of tuples are emitted, there is some risk that they
may overwhelm the downstream operators especially if they are performing some compute intensive
operation. For such cases, output can be throttled by reducing the value of the
`emitBatchSize` property. Conversely, if the downstream operators can handle the load, increase
the value to enhance throughput.

The `partitionCount` parameter has already been discussed above.

Occasionally, some files get into a bad state and cause errors when an attempt is made to
read from them. The causes vary depending on the filesystem type ranging from corrupted
filesystems to network issues. In such cases, the operator will retry reading from such
files a limited number of times before blacklisting those files. This retry count is
defined by the `maxRetryCount` property.

Finally, the specific scanner class used to monitor the input directories can be configured
by setting the `scanner` property.

| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *directory* | absolute path of directory to be scanned | String | Yes | N/A |
| *scanIntervalMillis* | Interval in milliseconds after which directory should be scanned for new files | int | No | 5000 |
| *emitBatchSize* | Maximum number of tuples to emit in a single call to the `emitTuples()` callback (see explanation above). | int | No | 1000 |
| *partitionCount* | Desired number of partitions | int | No | 1 |
| *maxRetryCount* | Maximum number of times the operator will attempt to process a file | int |No | 5 |
| *scanner* | Scanner to scan new files in directory | [DirectoryScanner](#DirectoryScanner) | No | DirectoryScanner |

#### <a name="DirectoryScanner"></a>Properties of DirectoryScanner
The directory scanner has one optional property: a regular expression to filter files
of interest. If absent, all files in the source directory are processed. It can be
set like this:

```xml
<property>
  <name>dt.operator.{OperatorName}.prop.scanner.filePatternRegexp</name>
  <value>/tmp/fileInput</value>
</property>
```

| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *filePatternRegexp* | regex to select files from input directory | String | No | N/A |


### Ports
This operator has no ports.

## Abstract Methods
As described above, concrete subclasses need to provide implementations for these two
methods:

```java
void emit(T tuple);
T readEntity();
```

Examples of implementations are in the `LineByLineFileInputOperator` operator and also in
the example at the end of this guide.

## Derived Classes

### 1. AbstractFTPInputOperator
The class is used to read files from FTP file system. As for the above abstract class, concrete
subclasses need to implement the
[readEntity](https://www.datatorrent.com/docs/apidocs/com/datatorrent/lib/io/fs/AbstractFileInputOperator.html#readEntity) and
[emit](https://www.datatorrent.com/docs/apidocs/com/datatorrent/lib/io/fs/AbstractFileInputOperator.html#emit) methods.

#### <a name="AbstractFTPInputOperatorProps"></a>Properties
This operator defines following additional properties beyond those defined in the
[parent class](#AbstractFileInputOperatorProps).

| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *host*| Hostname of ftp server.| String | Yes | N/A |
| *port*| Port of ftp server.| int | No | 21 (default ftp port) |
| *userName*| Username which is used for login to the server. | String | No | anonymous |
| *password*| Password which is used for login to the server. | String | No | gues |

#### Ports
This operator has no ports.

### 2. FTPStringInputOperator
This class extends AbstractFTPInputOperator and  implements abstract methods to read files available on FTP file system line by line.

#### <a name="FTPStringInputOperatorProps"></a>Properties
This operator defines no additional properties beyond those defined in the
[parent class](#AbstractFTPInputOperatorProps).

#### Ports
| **Port** | **Description** | **Type** | **Mandatory** |
| -------- | ----------- | ---- | ------------------ |
| *output* | Tuples that are read from file are emitted on this port | String | Yes |

### 3. AbstractParquetFileReader

Reads Parquet files from input directory using GroupReadSupport. Derived classes need to implement [convertGroup(Group)](https://www.datatorrent.com/docs/apidocs/com/datatorrent/contrib/parquet/AbstractParquetFileReader.html#convertGroup(Group)) method to convert Group to other type. Also it should implement  [readEntity()](https://www.datatorrent.com/docs/apidocs/com/datatorrent/lib/io/fs/AbstractFileInputOperator.html#readEntity()) and [emit(T)](https://www.datatorrent.com/docs/apidocs/com/datatorrent/lib/io/fs/AbstractFileInputOperator.html#emit(T)) methods.

#### <a name="AbstractParquetFileReaderProps"></a>Properties of AbstractParquetFileReader
This operator defines following additional properties beyond those defined in the
[parent class](#AbstractFileInputOperatorProps).

| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *parquetSchema*| Parquet Schema to parse record. | String | Yes | N/A |

#### Ports
This operator has no ports.

### 4. AbstractThroughputFileInputOperator

This operator extends `AbstractFileInputOperator` by providing the capability to partition
dynamically based the file backlog. The user can set the preferred number of pending files per operator as well as the maximum number of operators and define a re-partition interval. If a physical operator runs out of files to process and an amount of time greater than or equal to the repartition interval has passed then a new number of operators are created to accommodate the remaining pending files. Derived classes need to implement [readEntity()](https://www.datatorrent.com/docs/apidocs/com/datatorrent/lib/io/fs/AbstractFileInputOperator.html#readEntity()) and [emit(T)](https://www.datatorrent.com/docs/apidocs/com/datatorrent/lib/io/fs/AbstractFileInputOperator.html#emit(T)) methods.

#### <a name="AbstractThroughputFileInputOperatorProps"></a>Properties of AbstractThroughputFileInputOperator
This operator defines following additional properties beyond those defined in the
[parent class](#AbstractFileInputOperatorProps).

| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *repartitionInterval*| The minimum amount of time that must pass in milliseconds before the operator can be repartitioned. | long | No | 5 minutes |
| *preferredMaxPendingFilesPerOperator* | the preferred number of pending files per operator. | int | No | 10 |
| *partitionCount* | the maximum number of partitions for the operator. | int | No | 1 |

#### Ports
This operator has no ports.

### 5. LineByLineFileInputOperator
As mentioned in the overview above, this operator defines a single output port; it reads files
as lines and emits them as Java Strings on the output port. The output port *must* be connected.
Lines are extracted using the Java `BufferedReader` class and the default character encoding.
An example illustrating the use of a custom encoding (such as UTF_8) is provided below

#### Properties
This operator defines no additional properties beyond those defined in the
[parent class](#AbstractFileInputOperatorProps).

#### Ports
| **Port** | **Description** | **Type** | **Mandatory** |
| -------- | ----------- | ---- | ------------------ |
| *output* | Tuples that are read from file are emitted on this port | String | Yes |

## Example Implementation Using a Custom Character Encoding
This example demonstrates how to extend the `AbstractFileInputOperator` to read
UTF-8 encoded data.

```
public class EncodedDataReader extends AbstractFileInputOperator<String>
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();
  protected transient BufferedReader br;

  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    super.closeFile(is);
    br.close();
    br = null;
  }

  @Override
  protected String readEntity() throws IOException
  {
    return br.readLine();
  }

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
  }
}

```

## Common Implementation Scenarios
Sometimes, downstream operators need to know which file each tuple came from; there are a
number of ways of achieving this, each with its own tradeoffs. Some alternatives:

- If the generic tuple type is a String, each tuple can be prefixed with the file name
  with a suitable separator, for example: `foo.txt: first line`. This works but
  has obvious additional costs in both processing (to parse out the two pieces of each
  tuple) and network bandwidth utilization.
- Define a custom tuple class with two fields: one for the file name and one for tuple data.
  The costs are similar to the previous approach though the code is simpler since
  parsing is handled behind the scenes by the serialization process.
- Define the tuple type to be `Object` and emit either a custom `Tuple` object for actual
  tuple data or **BOF**/**EOF** objects with the name of the file when a new file begins
  or the current file ends. Here, the additional bandwidth consumed is
  minimal (just 2 additional tuples at file boundaries) but the type of each tuple needs
  to be checked using `instanceof` in the downstream operators which has some runtime cost.
- Similar to the previous approach but define an additional control port dedicated to
  the BOF/EOF control tuples. This approach eliminates the runtime cost of using `instanceof`
  but some care is needed because (a) the order of tuples arriving at multiple input ports
  in downstream operators cannot be guaranteed -- for example, the BOF/EOF control tuples
  may arrive before some of the actual data tuples; and (b) since the operator may read
  more than one file in a single streaming window, the downstream operator may not be
  able to tell which tuples belong to which file. One way of dealing with this is to
  stop emitting data tuples until the next `endWindow()` callback when an EOF is detected
  for the current file; that way, if the downstream operator receives an EOF control tuple,
  it has the guarantee that all the data tuples received in the same window belong to the
  current file.

Of course, other strategies are possible depending on the needs of the particular situation.

When used in a long-running application where a very large number of files are are processed
over time, the internal state (consisting of properties like `processedFiles`) may grow
correspondingly and this may have some performance impact since each checkpoint saves the
entire operator state. In such situations, it is useful to explore options such as moving
processed files to another directory and trimming operator state variables suitably.

