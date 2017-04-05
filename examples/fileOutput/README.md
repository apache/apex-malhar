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
