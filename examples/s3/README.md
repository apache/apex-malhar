## Amazon S3 to HDFS sync application

Ingest and backup Amazon S3 data to Hadoop HDFS for data download from Amazon to hadoop. 
  
This application transfers files from the configured S3 location to the destination path in HDFS.
The source code is available at: https://github.com/DataTorrent/examples/tree/master/tutorials/s3-to-hdfs-sync
Send feedback or feature requests to feedback@datatorrent.com

## S3 tuple output example

Sample application to show how to use the S3 tuple output module.

The application reads records from HDFS using `FSRecordReaderModule`. 
These records are then written to Amazon S3 using `S3BytesOutputModule`.

### How to configure
The properties file META-INF/properties.xml shows how to configure the respective operators.

### How to compile
`shell> mvn clean package`

This will generate application package s3-tuple-output-1.0-SNAPSHOT.apa inside target directory.

### How to run
Use the application package generated above to launch the application from UI console(if available) or apex command line interface.

`apex> launch target/s3-tuple-output-1.0-SNAPSHOT.apa`

Sample application to show how to use the S3OutputModule to upload files into Amazon S3 Bucket.

Operators in sample application are as follows:
1) FSInputModule which reads files from file systems HDFS/NFS and emits FileMetadata, BlockMetadata, BlockBytes.
2) S3OutputModule which uploads the files into S3 Bucket using multi-part upload feature.

Please configure the below S3OutputModule properties in src/main/resources/META-INF/properties.xml before launching the application:

-   ***accessKey*** -   String
    -   Specifies the AWS access key to access the Amazon S3 bucket.
    
-   ***secretAccessKey***   -   String
    -   Specifies the AWS secret access key to access the Amazon S3 bucket.
    
For more information about access key and secret access key, Please refer to [IAM](http://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
    
-   ***bucketName***    -   String
    -   Specifies the name of the S3 bucket to copy the files/directories.
    
-   ***outputDirectoryPath***   -   String
    -   Specifies the path of the output directory to copy the files/directories.
    
Suppose, **app.hdfs2s3** is the name of the **bucket** and you want to copy the files to S3 location (app.hdfs2s3/apex/s3output) then configure the properties as below:

```xml
  <property>
    <name>dt.operator.S3OutputModule.prop.bucketName</name>
    <value>app.hdfs2s3</value>
  </property>
  <property>
    <name>dt.operator.S3OutputModule.prop.outputDirectoryPath</name>
    <value>apex/s3output</value>
  </property>
```

## S3Output 

Sample application to show how to use the S3OutputModule to upload files into Amazon S3 Bucket.

Operators in sample application are as follows:
1) FSInputModule which reads files from file systems HDFS/NFS and emits FileMetadata, BlockMetadata, BlockBytes.
2) S3OutputModule which uploads the files into S3 Bucket using multi-part upload feature.

Please configure the below S3OutputModule properties in src/main/resources/META-INF/properties.xml before launching the application:

-   ***accessKey*** -   String
    -   Specifies the AWS access key to access the Amazon S3 bucket.
    
-   ***secretAccessKey***   -   String
    -   Specifies the AWS secret access key to access the Amazon S3 bucket.
    
For more information about access key and secret access key, Please refer to [IAM](http://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
    
-   ***bucketName***    -   String
    -   Specifies the name of the S3 bucket to copy the files/directories.
    
-   ***outputDirectoryPath***   -   String
    -   Specifies the path of the output directory to copy the files/directories.
    
Suppose, **app.hdfs2s3** is the name of the **bucket** and you want to copy the files to S3 location (app.hdfs2s3/apex/s3output) then configure the properties as below:

```xml
  <property>
    <name>dt.operator.S3OutputModule.prop.bucketName</name>
    <value>app.hdfs2s3</value>
  </property>
  <property>
    <name>dt.operator.S3OutputModule.prop.outputDirectoryPath</name>
    <value>apex/s3output</value>
  </property>
```
