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

**Step 2**: Update these properties in the file `src/site/conf/my-app-conf1.xml`:

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

Upload the `target/jmsActiveMQ-1.0-SNAPSHOT.apa` to the UI console if available or launch it from
the commandline using `apex`.

**Step 4**: During launch use `site/conf/my-app-conf1.xml` as a custom configuration file; then verify
that the output directory has the expected output:

    shell> hadoop fs -cat <HDFS output directory path>/jmsTestClstr*

This should return the contents of the "test1" queue. Note that if you used relative path such as target/
you should see the file in HDFS under /user/&lt;current user&gt;.

Sample Output:

    hadoop fs -cat <path_to_file>/jmsTestClstr.0
    message 1
    message 2
