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
