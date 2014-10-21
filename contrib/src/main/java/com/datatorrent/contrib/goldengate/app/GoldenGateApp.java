/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.app;

import java.util.HashSet;

import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.io.ConsoleOutputOperator;

import com.datatorrent.contrib.goldengate.GoldenGateQueryProcessor;
import com.datatorrent.contrib.goldengate.QueryProcessor;
import com.datatorrent.contrib.goldengate.lib.KafkaInput;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="GoldenGateDemo")
public class GoldenGateApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    SimpleKafkaConsumer simpleKafkaConsumer = new SimpleKafkaConsumer(Sets.newHashSet("node25.morado.com:9092"),
                                                                      "ggdemo",
                                                                      10000,
                                                                      100000,
                                                                      "ggdemo_client",
                                                                      new HashSet<Integer>());
    KafkaInput kafkaInput = new KafkaInput();
    //kafkaInput.setTopic("ggdemo");
    kafkaInput.setConsumer(simpleKafkaConsumer);
    dag.addOperator("kafkaInput", kafkaInput);

    ////

    ConsoleOutputOperator console = new ConsoleOutputOperator();
    dag.addOperator("console", console);

    ////

    dag.addStream("display", kafkaInput.outputPort, console.input);

    KafkaSinglePortStringInputOperator queryInput = dag.addOperator("QueryInput", KafkaSinglePortStringInputOperator.class);
    QueryProcessor queryProcessor = dag.addOperator("QueryProcessor", GoldenGateQueryProcessor.class);
    KafkaSinglePortOutputOperator<Object, Object> queryOutput = dag.addOperator("QueryResult", new KafkaSinglePortOutputOperator<Object, Object>());

    dag.addStream("queries", queryInput.outputPort, queryProcessor.queryInput);
    dag.addStream("results", queryProcessor.queryOutput, queryOutput.inputPort);
  }
}
