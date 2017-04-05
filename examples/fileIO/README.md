# Sample File Read-Write implementation
This example shows how to extend the `AbstractFileInputOperator` and `AbstractFileOutputOperator` from [Malhar library](https://github.com/apache/apex-malhar) to create a high performance application to copy files. There are two examples in the project:

1. FileIO: This application copies text file contents line by like to specified destination. The required properties for application are specified in src/main/resources/META-INF/properties-FileIO.xml.

2. ThroughputBasedFileIO: This application copies file contents block by block to specified destination. This application makes use of `AbstractThroughputFileInputOperator` from [Malhar library](https://github.com/apache/apex-malhar). The required properties for application are specified in src/main/resources/META-INF/properties-ThroughputBasedFileIO.xml.

#### Follow these steps to run this application

**Step 1**: Update input and output file(s)/folder properties in your application properties file.

For Input location update property `dt.operator.read.prop.directory`

For Output location update property `dt.operator.write.prop.filePath`

**Step 2**: Build the code:

    shell> mvn clean install

Upload the `target/fileIO-1.0-SNAPSHOT.apa` to the UI console if available or launch it from the commandline using `apexcli`.

**Step 3**: During launch use `src/main/resources/META-INF/properties-*.xml` as a custom configuration file; then verify
that the output directory has the expected output.

**Note**: The application can be run in local mode within your IDE by simply running tests available in `src/test/java` folder.
