/**
 * Put your copyright and license info here.
 */
package com.example.fileIO;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;

//import com.datatorrent.lib.io.fs.FileReaderMultiDir;
import static com.datatorrent.api.Context.PortContext.*;

@ApplicationAnnotation(name="FileIO")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // create operators
    FileReader reader = dag.addOperator("read",  FileReader.class);
    FileWriter writer = dag.addOperator("write", FileWriter.class);

    reader.setScanner(new FileReaderMultiDir.SlicedDirectoryScanner());

    // using parallel partitioning ensures that lines from a single file are handled
    // by the same writer
    //
    dag.setInputPortAttribute(writer.input, PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(writer.control, PARTITION_PARALLEL, true);

    dag.addStream("data", reader.output, writer.input);
    dag.addStream("ctrl", reader.control, writer.control);
  }
}
