## FTP Input Operator Example

This example shows how to use `FTPStringInputOperator` which is inherited from the `AbstractFTPInputOperator`. The `FTPStringInputOperator` scans a directory from an FTP server for files, reads lines from the files and
emits them on the output port for further processing. The tuples emitted by the `FTPStringInputOperator` are processed by the downstream operator `StringFileOutputOperator` which writes them to hdfs.

The properties file `META-INF/properties.xml` shows how to configure the respective operators.


Users can choose the application and additional configuration file to use during launch time. In this example, we use the files mentioned above to configure the operator properties.


#### **Update Properties from properties.xml - This is needed to run the example:**

- Update these common properties in the file `/src/main/resources/META-INF/properties.xml`:

| Property Name  | Description |
| -------------  | ----------- |
| dt.application.FTPInputExample.operator.Reader.host | address of the ftp server |
| dt.application.FTPInputExample.operator.Reader.userName | user for the ftp server if anonymous ftp is disabled |
| dt.application.FTPInputExample.operator.Reader.password | password associated with the above user |
| dt.application.FTPInputExample.operator.Writer.filePath | output file path for the records after formatting |
| dt.application.FTPInputExample.operator.Writer.outputFileName | output file name for the records to be written after formatting |

### How to compile
`shell> mvn clean package`

This will generate application package malhar-examples-ftp-3.8.0-SNAPSHOT.apa inside target directory.

### How to run
Use the application package generated above to launch the application from UI console(if available) or apex command line interface.

`apex> launch target/malhar-examples-ftp-3.8.0-SNAPSHOT.apa`




In case you have issues configuring the operator or running the application, please send an email to users@apache.apex.org.
