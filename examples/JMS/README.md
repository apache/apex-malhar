## Sample jmsActiveMQ Read Application

This application reads from a queue in `ActiveMQ`, creates one line per message and writes these lines to a file
in the user specified directory in HDFS.

Follow these steps to run this application:

**Step 1**: Set up the ActiveMQ Broker and the Queue to read from

Unless you already have the ActiveMQ broker installed, download, install and
start it as per http://activemq.apache.org/version-5-getting-started.html.
For this example, let us assume the broker is installed on a machine with the IP address
192.168.128.142. You can access the Admin UI as
http://192.168.128.142:8161/admin/ (or http://localhost:8161/admin/ on the
machine itself). Use the Queues tab to create a queue with some name
and use the Send tab to send a few messages to that queue. Let us assume the
queue name is "test1" (without the double quotes) for this example. You can
verify the presence of those messages by browsing the queue in the Admin UI.

**Step 2**: Update these properties in the file `src/main/resources/META-INF/properties.xml`:

| Property Name  | Description |
| -------------  | ----------- |
| dt.operator.fileOut.prop.filePath | HDFS output directory path e.g. absolute path such as /tmp or relative path such as target/ |
| dt.operator.fileOut.prop.baseName | Base name of the output file e.g. jmsTestClstr |
| dt.operator.fileOut.prop.maxLength | Maximum length of the output file in bytes after which the file is rolled e.g. 45 |
| dt.operator.fileOut.prop.rotationWindows   | Number of windows to elapse before the output file is rolled e.g. 1 |
| dt.operator.amqIn.prop.connectionFactoryProperties.brokerURL   | ActiveMQ Broker URL e.g. tcp://192.168.128.142:61616 |
| dt.operator.amqIn.prop.subject   | Name of the queue to read from e.g. test1 |

**Step 3**: Build the code:

    shell> mvn clean package -DskipTests

This will generate application package `jmsActiveMQ-1.0-SNAPSHOT.apa` inside target directory.

**Step 4**: Use the application package generated above to launch the application from 'apex' command line interface.
                        
    `apex> launch target/jmsActiveMQ-1.0-SNAPSHOT.apa`

During launch use `src/main/resources/META-INF/properties.xml` as a custom configuration file; then verify
that the output directory has the expected output:

    shell> hadoop fs -cat <HDFS output directory path>/jmsTestClstr*

This should return the contents of the "test1" queue. Note that if you used relative path such as target/
you should see the file in HDFS under /user/&lt;current user&gt;.

Sample Output:

    hadoop fs -cat <path_to_file>/jmsTestClstr.0
    message 1
    message 2

## Sample SQS implementation

This application reads from a queue in `Amazon SQS`, creates one line per message and writes these lines to a file
in the user specified directory in HDFS.

Follow these steps to run this application:

**Step 1**: Set up the Amazon SQS Queue to read from

It is assumed that you have an Amazon AWS account and an IAM user available for this example. Let's assume the
IAM user name is "sqsTest". Ensure that "sqsTest" has "AmazonSQSFullAccess" permission and the access keys are
available. Log in to your Amazon AWS account and in the AWS Management console click on the SQS option
(under Application Services) to go to the SQS console. Use the "Create New Queue" button to create a queue with
all default values. Let's assume the name of this queue is "DtQueue". Go to the permissions tab and ensure that
all IAM users of the current AWS account have access to this queue. Using "Queue Actions" drop-down and the
"Send a Message" action create a number of messages to be consumed by this application.

Record the accessKey and secretKey values for the IAM user "sqsTest" that will be required in the next step.
You also need to know the AWS region where the SQS queue has been created. Let's assume this is "us-east-1" for
this example. You can locate the region in the AWS console top right corner.


**Step 2**: Update the following properties in the file `src/main/resources/META-INF/properties-jmsSqs.xml`:

| Property Name  | Description |
| -------------  | ----------- |
| dt.operator.fileOut.prop.filePath | HDFS output directory path e.g. absolute path such as /tmp or relative path such as target/ |
| dt.operator.fileOut.prop.baseName | Base name of the output file e.g. jmsTestClstr |
| dt.operator.fileOut.prop.maxLength | Maximum length of the output file in bytes after which the file is rolled e.g. 50 |
| dt.operator.fileOut.prop.rotationWindows | Number of windows to elapse before the output file is rolled e.g. 10 |
| dt.operator.sqsIn.prop.subject | Name of the queue to read from (`DtQueue` for this example)  |
| dt.operator.sqsIn.prop.aws.key.id | Access key value for the IAM user "sqsTest" described above  |
| dt.operator.sqsIn.prop.aws.key.secret | Secret key value for the IAM user "sqsTest" described above |
| dt.operator.sqsIn.prop.aws.region | AWS region where the queue has been created e.g. "us-east-1" |


**Step 3**: Build the code:

    shell> mvn clean package -DskipTests

This will generate application package `jmsSqs-1.0-SNAPSHOT.apa` inside target directory.

Upload the generated package `jmsSqs-1.0-SNAPSHOT.apa` to launch it from the commandline using `apex`.

**Step 4**: Use the application package generated above to launch the application from 'apex' command line interface.
            
    `apex> launch target/jmsSqs-1.0-SNAPSHOT.apa`

During launch use `src/main/resources/META-INF/properties-jmsSqs.xml` as a custom configuration file; then verify
that the output directory has the expected output:

    shell> hadoop fs -cat <HDFS output directory path>/jmsTestClstr*

This should return the contents of the "DtQueue" queue. Note that if you used relative path such as target/
you should see the file in HDFS under /user/&lt;current user&gt;.

Sample Output:

    hadoop fs -cat <path_to_file>/jmsTestClstr.0
    message 1
    message 2
