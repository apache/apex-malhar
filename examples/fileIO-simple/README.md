Sample application to show how to use the file input and output operators.

During a typical run on a Hadoop cluster, when input files are dropped into the
configured input directory (e.g. `/tmp/SimpleFileIO/input-dir`), the application
will create temporary files like this at the configured output location in
HDFS (e.g. `/tmp/SimpleFileIO/output-dir`) and copy all input file data to it:

    /tmp/SimpleFileIO/output-dir/myfile_p2.0.1465929407447.tmp

When the file size exceeds the configured limit of 100000 bytes, a new file with
a name like `myfile_p2.1.1465929407447.tmp` will be opened and, a minute or two
later, the old file will be renamed to `myfile_p2.0`.
