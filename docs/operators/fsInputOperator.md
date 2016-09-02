File Input Operator
=============

## Operator Objective
This operator scans a directory for files. Files are then read and split into tuples, which are emitted. The default implementation scans a single directory. The operator is fault tolerant. It tracks previously read files and current offset as part of checkpoint state. In case of failure the operator will skip files that were already processed and fast forward to the offset of the current file. Supports partitioning and changes to number of partitions. The directory scanner is responsible to only accept the files that belong to a partition.

File Input Operator is **idempotent**, **fault-tolerant** and **partitionable**.

## Operator Usecase
1. Read all files of a directory and then keep scanning it for newly added files.

## Operator Information
1. Operator location: ***malhar-library***
2. Available since: ***1.0.2***
3. Operator state: ***Stable***
3. Java Packages:
    * Operator: ***[com.datatorrent.lib.io.fs.AbstractFileInputOperator](https://www.datatorrent.com/docs/apidocs/com/datatorrent/lib/io/fs/AbstractFileInputOperator.html)***

### AbstractFileInputOperator
This is the abstract implementation that serves as base class for scanning a directory for files and read the files one by one. This class doesn’t have any ports.

![AbstractFileInputOperator.png](images/fsInput/operatorsClassDiagram.png)

## Properties, Attributes and Ports
### <a name="props"></a>Properties of AbstractFileInputOperator
| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *directory* | absolute path of directory to be scanned | String | Yes | N/A |
| *scanIntervalMillis* | Interval in milliseconds after which directory should be scanned for new files | int | No | 5000 |
| *emitBatchSize* | Number of tuples to emit in a batch | int | No | 1000 |
| *partitionCount* | Desired number of partitions count | int | No | 1 |
| *maxRetryCount* | Maximum number of times the operator will attempt to process a file | No | 5 |
| *scanner* | Scanner to scan new files in directory | [DirectoryScanner](#DirectoryScanner) | No | DirectoryScanner |

#### <a name="JDBCStore"></a>Properties of DirectoryScanner
| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *filePatternRegexp* | regex to select files from input directory | String | No | N/A |

## Abstract Methods
void emit(T tuple): Abstract method that emits tuple read from file.
T readEntity(): Abstract method to read file entity (can be a line or block).

## Partitioning of File Input Operator
Configure parameter "partitionCount" to define desired number of partitions.

```xml
  <property>
    <name>dt.operator.{OperatorName}.prop.partitionCount</name>
    <value>4</value>
  </property>
```

where {OperatorName} is the name of the input operator.
Above lines will partition operator statically 4 times. Above value can be changed accordingly to change the number of static partitions.

#### Dynamic Partitioning of JDBC Poller
Implement ```getNewPartitionCount()``` method to do dymanic partitioning.

## Derived Classes
### 1. AbstractFTPInputOperator
The class is used to read files from FTP file system.
#### Properties, Attributes and Ports
#### <a name="props"></a>Properties of AbstractFTPInputOperator
| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *host*| Hostname of ftp server.| String | Yes | N/A |
| *port*| Port of ftp server.| int | No | 21 (default ftp port) |
| *userName*| Username which is used for login to the server. | String | No | anonymous |
| *password*| Password which is used for login to the server. | String | No | gues |

### 2. AbstractParquetFileReader
Reads Parquet files from input directory using GroupReadSupport. Derived classes need to implement [convertGroup(Group)](https://www.datatorrent.com/docs/apidocs/com/datatorrent/contrib/parquet/AbstractParquetFileReader.html#convertGroup(Group)) method to convert Group to other type.

#### Properties, Attributes and Ports
#### <a name="props"></a>Properties of AbstractParquetFileReader
| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *parquetSchema*| Parquet Schema to parse record. | String | Yes | N/A |

### 3. AbstractThroughputFileInputOperator
This operator provides dynamic partitioning to AbstractFileInputOperator. The user can set the preferred number of pending files per operator as well as the max number of operators and define a repartition interval. If a physical operator runs out of files to process and an amount of time greater than or equal to the repartition interval has passed then a new number of operators are created to accommodate the remaining pending files.

#### Properties, Attributes and Ports
#### <a name="props"></a>Properties of AbstractThroughputFileInputOperator
| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *repartitionInterval*| The minimum amount of time that must pass in milliseconds before the operator can be repartitioned. | long | No | 5 minutes |
| *preferredMaxPendingFilesPerOperator* | the preferred number of pending files per operator. | int | No | 10 |
| *partitionCount* | the maximum number of partitions for the operator. | int | No | 1 |

### 4. LineByLineFileInputOperator
The operator read contents of a file line by line. Each line is emitted as a separate tuple in string format.

#### Properties, Attributes and Ports
#### Ports
| **Port** | **Description** | **Type** | **Mandatory** |
| -------- | ----------- | ---- | ------------------ |
| *output* | Tuples that are read from file are emitted on this port | String | No |


