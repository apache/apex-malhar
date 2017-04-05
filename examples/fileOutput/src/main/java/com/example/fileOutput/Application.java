package com.example.fileOutput;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="fileOutput")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    SequenceGenerator generator = dag.addOperator("generator", SequenceGenerator.class);

    FileWriter writer = dag.addOperator("writer", FileWriter.class);

    // properties can be set here or from properties file
    //writer.setMaxLength(1 << 10);

    dag.addStream("data", generator.out, writer.input);
  }
}
