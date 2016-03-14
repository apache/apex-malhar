package com.example.myapexapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="TestStuff")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomNumberGenerator random = dag.addOperator("randomInt",     RandomNumberGenerator.class);
    TestPartition testPartition  = dag.addOperator("testPartition", TestPartition.class);
    Codec3 codec = new Codec3();
    dag.setInputPortAttribute(testPartition.in, PortContext.STREAM_CODEC, codec);

    //Add locality if needed, e.g.: .setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("randomData", random.out, testPartition.in);
  }
}
