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


**Step 2**: Update the following properties in the file `src/site/conf/my-app-conf1.xml`:

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

Upload the generated package `target/jmsSqs-1.0-SNAPSHOT.apa` to the UI console if available or launch it from
the commandline using `apex`.

**Step 4**: During launch use `site/conf/my-app-conf1.xml` as a custom configuration file; then verify
that the output directory has the expected output:

    shell> hadoop fs -cat <HDFS output directory path>/jmsTestClstr*

This should return the contents of the "DtQueue" queue. Note that if you used relative path such as target/
you should see the file in HDFS under /user/&lt;current user&gt;.

Sample Output:

    hadoop fs -cat <path_to_file>/jmsTestClstr.0
    message 1
    message 2
