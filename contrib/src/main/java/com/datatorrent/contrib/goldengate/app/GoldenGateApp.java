/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.app;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.goldengate.lib.KafkaInput;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.google.common.collect.Sets;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;

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
    kafkaInput.setTopic("ggdemo");
    kafkaInput.setConsumer(simpleKafkaConsumer);
    dag.addOperator("kafkaInput", kafkaInput);

    ////

    ConsoleOutputOperator console = new ConsoleOutputOperator();
    dag.addOperator("console", console);

    ////

    dag.addStream("display", kafkaInput.outputPort, console.input);
  }
}
