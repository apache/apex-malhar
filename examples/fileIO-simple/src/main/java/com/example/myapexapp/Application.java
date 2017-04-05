package com.example.myapexapp;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;

/**
 * Simple application illustrating file input-output
 */
@ApplicationAnnotation(name="SimpleFileIO")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // create operators
    LineByLineFileInputOperator in = dag.addOperator("input",
                                               new LineByLineFileInputOperator());
    FileOutputOperator out = dag.addOperator("output",
                                             new FileOutputOperator());
    // configure operators
    in.setDirectory("/tmp/SimpleFileIO/input-dir");
    out.setFilePath("/tmp/SimpleFileIO/output-dir");
    out.setMaxLength(1_000_000);        // file rotation size

    // create streams
    dag.addStream("data", in.output, out.input);
  }
}
