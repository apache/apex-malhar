## Sample application for File Read-Write implementation

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

## Sample application for Multi Directory File Read-Write Implementation

This example is very similar to the fileIO example with one difference: it shows how
create a set of partitions separated into slices where each slice monitors a different
input directory. A custom partitioner and directory scanner are used.

## Sample application for File Input &  output operators

Sample application to show how to use the file input and output operators.

During a typical run on a Hadoop cluster, when input files are dropped into the
configured input directory (e.g. `/tmp/SimpleFileIO/input-dir`), the application
will create temporary files like this at the configured output location in
HDFS (e.g. `/tmp/SimpleFileIO/output-dir`) and copy all input file data to it:

    /tmp/SimpleFileIO/output-dir/myfile_p2.0.1465929407447.tmp

When the file size exceeds the configured limit of 100000 bytes, a new file with
a name like `myfile_p2.1.1465929407447.tmp` will be opened and, a minute or two
later, the old file will be renamed to `myfile_p2.0`.

## Sample application for file output operator with partitioning and rolling file output.

Sample application to show how to use the file output operator along with
partitioning and rolling file output.

A typical run on a Hadoop cluster will create files like this at the configured
output location in HDFS (e.g. `/tmp/fileOutput`) where the numeric extension is
the sequnce number of rolling output files and the number following 'p' is the
operator id of the partition that generated the file:

    /tmp/fileOutput/sequence_p3.0
    /tmp/fileOutput/sequence_p3.1
    /tmp/fileOutput/sequence_p4.0
    /tmp/fileOutput/sequence_p4.1

Each file should contain lines like this where the second value is the number
produced by the generator operator and the first is the corresponding operator id:

    [1, 1075]
    [1, 1095]
    [2, 1110]
    [2, 1120]

Please note that there are no guarantees about the way operator ids are assigned
to operators by the platform.

