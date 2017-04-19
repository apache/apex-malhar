# S3 tuple output example

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
