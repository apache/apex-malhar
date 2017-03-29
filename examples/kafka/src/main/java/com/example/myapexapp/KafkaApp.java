package com.example.myapexapp;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="Kafka2HDFS")
public class KafkaApp implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortInputOperator in
      = dag.addOperator("kafkaIn", new KafkaSinglePortInputOperator());

    in.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());
    LineOutputOperator out = dag.addOperator("fileOut", new LineOutputOperator());

    dag.addStream("data", in.outputPort, out.input);
  }
}
