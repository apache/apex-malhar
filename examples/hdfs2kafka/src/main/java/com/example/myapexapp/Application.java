package com.example.myapexapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

@ApplicationAnnotation(name="Hdfs2Kafka")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    LineByLineFileInputOperator in = dag.addOperator("lines",
                                                     LineByLineFileInputOperator.class);

    KafkaSinglePortOutputOperator<String,String> out = dag.addOperator("kafkaOutput", new KafkaSinglePortOutputOperator<String,String>());

    dag.addStream("data", in.output, out.inputPort).setLocality(Locality.CONTAINER_LOCAL);
  }
}
