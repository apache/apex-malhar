package com.example.dynamic;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;

import com.datatorrent.lib.stream.DevNull;

@ApplicationAnnotation(name="Dyn")
public class App implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Gen gen         = dag.addOperator("gen",     Gen.class);
    DevNull devNull = dag.addOperator("devNull", DevNull.class);

    dag.addStream("data", gen.out, devNull.data);
  }
}
