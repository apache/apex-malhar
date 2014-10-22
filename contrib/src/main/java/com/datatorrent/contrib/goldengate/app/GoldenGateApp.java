/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.app;

import java.util.HashSet;

import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.db.jdbc.JdbcStore;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import com.datatorrent.contrib.goldengate.GoldenGateQueryProcessor;
import com.datatorrent.contrib.goldengate.QueryProcessor;
import com.datatorrent.contrib.goldengate.lib.KafkaInput;
import com.datatorrent.contrib.goldengate.lib.OracleDBOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import java.util.Properties;

//@ApplicationAnnotation(name="GoldenGateDemo")
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
    kafkaInput.setConsumer(simpleKafkaConsumer);
    dag.addOperator("kafkaInput", kafkaInput);

    ////

    JdbcStore store = new JdbcStore();
    store.setDbDriver("oracle.jdbc.driver.OracleDriver");
    store.setDbUrl("jdbc:oracle:thin:@node25.morado.com:1521:xe");
    store.setConnectionProperties("user:ogguser,password:dt");

    OracleDBOutputOperator db = new OracleDBOutputOperator();
    db.setStore(store);
    dag.addOperator("oracledb", db);

    ////

    ConsoleOutputOperator console = new ConsoleOutputOperator();
    dag.addOperator("console", console);

    ////

    dag.addStream("display", kafkaInput.outputPort, console.input);
    dag.addStream("inputtodb", kafkaInput.employeePort, db.input);

    ////

    KafkaSinglePortStringInputOperator queryInput = dag.addOperator("QueryInput", KafkaSinglePortStringInputOperator.class);

    SimpleKafkaConsumer inputConsumer = new SimpleKafkaConsumer(Sets.newHashSet("node25.morado.com:9092"),
                                                                      "GoldenGateQuery",
                                                                      10000,
                                                                      100000,
                                                                      "GoldenGateQuery_client",
                                                                      new HashSet<Integer>());

    queryInput.setConsumer(inputConsumer);

    //

    GoldenGateQueryProcessor queryProcessor = dag.addOperator("QueryProcessor", GoldenGateQueryProcessor.class);

    JdbcStore queryStore = new JdbcStore();
    store.setDbDriver("oracle.jdbc.driver.OracleDriver");
    store.setDbUrl("jdbc:oracle:thin:@node25.morado.com:1521:xe");
    store.setConnectionProperties("user:ogguser,password:dt");
    //queryProcessor.setStore(queryStore);

    //

    KafkaSinglePortOutputOperator<Object, Object> queryOutput = dag.addOperator("QueryResult", new KafkaSinglePortOutputOperator<Object, Object>());

    Properties configProperties = new Properties();
    configProperties.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    configProperties.setProperty("metadata.broker.list", "node25.morado.com:9092");
    queryOutput.setConfigProperties(configProperties);

    dag.addStream("queries", queryInput.outputPort, queryProcessor.queryInput);
    dag.addStream("results", queryProcessor.queryOutput, queryOutput.inputPort);
  }
}
