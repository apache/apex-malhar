S3OutputModule
==============

### About Amazon S3
-------------------

**Amazon S3 (Simple Storage Service)** is an object storage system with a web service interface to store and retrieve any amount of data at any time from anywhere on the web, offered by Amazon Web Services.

### S3 Output Module
--------------------------

Purpose of S3Output module is to upload files/directories into an Amazon S3 bucket using the multipart upload feature(see below).

S3Output module is **fault-tolerant**, **statically/dynamically partitionable** and has **exactly once** semantics.

Module class is **S3OutputModule** located in the package **org.apache.apex.malhar.lib.fs.s3**; please refer to [github URL](https://github.com/apache/apex-malhar/blob/master/library/src/main/java/org/apache/apex/malhar/lib/fs/s3/S3OutputModule.java).

### Overview
------------

File upload to S3 can also be done using **AbstractFileOutputOperator** but that operator uploads large files sequentially; the current module in contrast can substantially improve the upload speed of large files by reading and uploading their constituent blocks in parallel. 

The table below lists additional benefits of this module over **AbstractFileOutputOperator**.

**S3OutputModule**|**AbstractFileOutputOperator**
-----|-----
Maximum upload file size is 5TB.|Maximum upload file size is 5GB.
Best fit for both large and small files.|Best fit for small files.
Module uses AmazonS3Client API's to upload objects into S3. Large files will upload using multipart feature and small files(single block) will upload using **putObject(...)** API|Operator uses Hadoop filesystems like **S3AFileSystem**. Consists of couple of steps to upload object into S3: (1) Write the data into the local filesystem. (2) When the stream closes, filesystem uploads the local object into S3.
If a block fails to upload then you need to re-upload the data for that block only|If a file fails to upload then you need to re-upload the complete file.

### Multipart Upload Feature
----------------------------

Uploading parts of a file is done via the [multipart feature](http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html); using this feature, each part of a file can be uploaded independently.
After all parts of a file are uploaded successfully, Amazon S3 combines the parts as a single object.

Please refer to the [Java code](http://docs.aws.amazon.com/AmazonS3/latest/dev/llJavaUploadFile.html) for uploading file into Amazon S3 bucket using multipart feature.

### Module
----------

A **module** is a group of operators pre-wired together so they work as a single conceptual entity in an application. Typically, a module will contain a set of input ports, output ports and configuration properties. The operators internal to the module will be automatically configured based on the supplied module properties.

### Operators in S3OutputModule
-----------------------------------------

Following diagram illustrates the DAG in this module:

![](images/s3output/s3outputmodule.png)

-   ***S3InitiateFileUploadOperator***
    -   Initiate the upload for the file using **AmazonS3Client.initiateMultipartUpload(...)** method only if the number of blocks for a file is greater than 1. By successfully initiating the upload, S3 returns a response of type **InitiateMultipartUploadResult**, which includes the **upload ID**, which is the unique identifier for the multipart upload. This **upload ID** must be included in each operation like block upload and upload completion request.
If the file has single block then the operator emits an empty string, this is an indication to downstream operators to not use the multi-part feature.
    -   This operator emits the pair **(filemetadata, uploadId)** to **S3FileMerger** and the triple **(filePath, metadata, uploadId)** to **S3BlockUploadOperator**.

-   ***S3BlockUploadOperator***
    -   This operator upload the blocks into S3 using different calls which depend on number of blocks of a file.
If the file has single block then upload the block using **AmazonS3Client.putObject(...)** call. S3 returns a response of type **PutObjectResult** which includes the **ETag**.
If the file has more blocks then upload the block using **AmazonS3Client.uploadPart(...)** call. S3 returns a response of type **UploadPartResult** which includes the **ETag**. This **ETag** value must be included in the request to complete multipart upload.
    -   **S3BlockUploadOperator** emits the pair **(path, ETag)** to **s3FileMerger**.

-   ***S3FileMerger***
    -   Complete multipart upload request using **AmazonS3Client.completeMultipartUpload(...)**. This call must include the **upload ID** and a list of both part numbers and corresponding **ETag** values. **S3FileMerger** sends the complete multi-part upload request to S3 once it has all the **part ETag's** of a file. 
**Amazon S3** creates an object by concatenating the parts in ascending order based on part number. After a successful upload request, the parts no longer exist and S3 response includes an **ETag** which uniquely identifies the combined object data. 

### Configuration Parameters
-----------------------------

-   ***accessKey*** -   String
    -   Mandatory Parameter    
    -   Specifies the AWS access key to access Amazon S3 and has permissions to access the specified bucket.
    -   Example value = AKIAJVAGFANC2LSZCJ4Q

-   ***secretAccessKey***   -   String
    -   Mandatory Parameter
    -   Specifies the AWS secret access key to access Amazon S3 and has permissions to access the specified bucket.
    -   Example value = wpVr3U82RmCKJoY007YfkaawT7CenhTcK1B8clue
    
-   ***endPoint***  -   String
    -   Endpoint is the URL for the entry point for a web service. Specify the valid endpoint to access S3 bucket.
    -   This is an optional parameter. If the bucket is accessed only from specific end point then the user has to specify this parameter.
    -   Please refer to [endPoint](http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region) table about the endpoints supported by S3. 
    -   Example value = s3.amazonaws.com 

-   ***bucketName***    -   String
    -   Mandatory Parameter
    -   S3 buckets are used to store objects which consists of data and metadata that describes the data. Specify the name of the bucket.
    -   Example value = apex.app.test.s3

-   ***outputDirectoryPath***   -   String
    -   Mandatory Parameter
    -   Specifies the path of the output directory. 
    -   Example value = dt/app/output

-   ***mergerCount***   -   int
    -   Specify the number of instances of S3FileMerger operator.
    -   Default value = 1
    
-   ***timeOutWIndowCount***    -   int
    -   This property maps to the [OperatorContext.TIMEOUT_WINDOW_COUNT](https://github.com/apache/apex-core/blob/master/api/src/main/java/com/datatorrent/api/Context.java) attribute and is a count of streaming windows. If specified, it will be set on all the operators of this module. Since these operators interact with S3, there may be additional latencies that cause the platform to kill them because they are considered stalled. Increasing this value prevents this and allows the application to proceed despite the latencies.
    -   Default value = 6000

### Ports
---------

-   ***filesMetadataInput***    -   AbstractFileSplitter.FileMetadata
    -   Input port for files metadata.
    -   Mandatory

-   ***blocksMetadataInput***   -   BlockMetadata.FileBlockMetadata
    -   Input port for blocks metadata.
    -   Mandatory
    
-   ***blockData*** -   AbstractBlockReader.ReaderRecord<Slice>
    -   Input port for blocks data.
    -   Mandatory
    
### Application Example
-----------------------

Please refer to [Example](https://github.com/DataTorrent/examples/tree/master/tutorials/s3output) for S3OutputModule sample application.

### Partitioning
----------------

Partitioning the module means that the operators in the module can be partitioned.

#### Stateless Partitioning
---------------------------

Partitioning the operator in module can be achieved as follows:

##### S3InitiateFileUploadOperator
----------------------------------

Partition of this operator is achieved indirectly as follows:
```xml
<property>
  <name>dt.operator.{ModuleName}#InitiateUpload.attr.PARTITIONER</name>
  <value>com.datatorrent.common.partitioner.StatelessPartitioner:{N}</value>
</property>     
```
where {ModuleName} is the name of the S3OutputModule and
      {N} is the number of static partitions.
Above lines will partition S3InitiateFileUploadOperator statically {N} times.

##### S3BlockUploadOperator
---------------------------

Locality of S3BlockUploadOperator with upstream operator (FSInputModule/BlockReader) must set to PARTITION_PARALLEL for performance benefits by avoiding serialization/deserialization of objects. So, partitioning of this operator depends on upstream operator which is of type FSInputModule/BlockReader.
 
##### S3FileMerger
------------------
    
By setting the parameter "mergerCount", **S3FileMerger** be statically partitioned. This can be achieved by two ways:

(a) Following code can be added to populateDAG(DAG dag, Configuration conf) method of application to statically partitioning **S3FileMerger** {N} times:
    
```java
  FSInputModule inputModule = dag.addModule("HDFSInputModule", new FSInputModule());
  S3OutputModule outputModule = dag.addModule("S3OutputModule", new S3OutputModule());
  outputModule.setMergerCount({N});
```
    
(b) By setting the parameter in properties file as follows

```xml
  <property>
    <name>dt.operator.{ModuleName}.prop.mergerCount</name>
    <value>{N}</value>
  </property>
```
where {ModuleName} is the name of the S3OutputModule and {N} is the number of static partitions.
Above lines will partition **S3FileMerger** statically {N} times.          
         
#### Dynamic Partitioning
--------------------------

Dynamic partitioning is a feature of Apex platform which changes the number of partitions of an operator at run time.
Locality of **S3BlockUploadOperator** with upstream operator(FSInputModule/BlockReader) must set to PARTITION_PARALLEL for performance benefits by avoiding serialization/deserialization of objects. So, dynamic partitioning of this operator depends on upstream operator which is of type FSInputModule/BlockReader.

From the example application, by setting the maxReaders and minReaders value to FSInputModule, **S3BlockUploadOperator** dynamically partitioned between minReaders and maxReaders. This can be achieved by two ways:
(a) Following code can be added to **populateDAG(DAG dag, Configuration conf)** method of application to dynamically partitioned **S3BlockUploadOperator** between {N1} and {N2} times:
                                                                                                                                                                                
```java
FSInputModule inputModule = dag.addModule("HDFSInputModule", new FSInputModule());
inputModule.setMinReaders({N1});
inputModule.setMaxReaders({N2});
S3OutputModule outputModule = dag.addModule("S3OutputModule", new S3OutputModule());
```

(b) By setting the parameter in properties file as follows:

```xml
<property>
  <name>dt.operator.HDFSInputModule.prop.minReaders</name>
  <value>{N1}</value>
</property>
<property>
  <name>dt.operator.HDFSInputModule.prop.maxReaders</name>
  <value>{N2}</value>
</property>         
```

{N1} and {N2} represents the number of minimum and maximum partitions of BlockReader.
Above lines will dynamically partitioned the **S3BlockUploadOperator** between {N1} and {N2} times. 
