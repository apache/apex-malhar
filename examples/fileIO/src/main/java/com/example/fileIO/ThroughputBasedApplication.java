package com.example.fileIO;

import static com.datatorrent.api.Context.PortContext.PARTITION_PARALLEL;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "ThroughputBasedFileIO")
public class ThroughputBasedApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    ThroughputBasedReader reader = dag.addOperator("read", ThroughputBasedReader.class);
    BytesFileWriter writer = dag.addOperator("write", BytesFileWriter.class);

    dag.setInputPortAttribute(writer.input, PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(writer.control, PARTITION_PARALLEL, true);

    dag.addStream("data", reader.output, writer.input);
    dag.addStream("ctrl", reader.control, writer.control);
  }

}
