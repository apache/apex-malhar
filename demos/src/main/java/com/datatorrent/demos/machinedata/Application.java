/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.machinedata;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.Operator.OutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.demos.machinedata.data.MachineKey;
import com.datatorrent.demos.machinedata.operator.CalculatorOperator;
import com.datatorrent.demos.machinedata.operator.MachineInfoAveragingOperator;
import com.datatorrent.demos.machinedata.operator.MachineInfoAveragingPrerequisitesOperator;
import com.datatorrent.contrib.redis.RedisKeyValPairOutputOperator;
import com.datatorrent.contrib.redis.RedisMapOutputOperator;
import com.datatorrent.contrib.redis.RedisStore;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.SmtpOutputOperator;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Resource monitor application.
 * </p>
 *
 * @since 0.3.5
 */
@ApplicationAnnotation(name = "MachineDataDemo")
@SuppressWarnings("unused")
public class Application implements StreamingApplication
{

  private static final Logger LOG = LoggerFactory.getLogger(Application.class);
  protected int streamingWindowSizeMilliSeconds = 1000; // 1 second
  protected int appWindowCountMinute = 5; // 10 seconds
  protected int compareAlertWindow = 15; // 1/4 minute
  private boolean isWebsocket = false;
  private int QUEUE_CAPACITY = 32 * 1024;

  /**
   * This method returns new InputReceiver Operator
   *
   * @param name the name of the operator in DAG
   * @param dag  the DAG instance
   * @return InputReceiver
   */
  private InputReceiver getRandomInformationTupleGenerator(String name, DAG dag)
  {
    InputReceiver oper = dag.addOperator(name, InputReceiver.class);
    dag.setAttribute(oper, Context.OperatorContext.APPLICATION_WINDOW_COUNT, appWindowCountMinute);
    return oper;
  }

  /**
   * This method returns new MachineInfoAveragingPrerequisitesOperator Operator
   *
   * @param name the name of the operator in DAG
   * @param dag  the DAG instance
   * @return MachineInfoAveragingPrerequisitesOperator
   */
  private MachineInfoAveragingPrerequisitesOperator getMachineInfoAveragingPrerequisitesOperator(String name, DAG dag)
  {
    MachineInfoAveragingPrerequisitesOperator oper = dag.addOperator(name, MachineInfoAveragingPrerequisitesOperator.class);
    dag.setAttribute(oper, Context.OperatorContext.APPLICATION_WINDOW_COUNT, appWindowCountMinute);
    return oper;
  }

  /**
   * This method returns new MachineInfoAveragingOperator Operator
   *
   * @param name the name of the operator in DAG
   * @param dag  the DAG instance
   * @return MachineInfoAveragingOperator
   */
  private MachineInfoAveragingOperator getMachineInfoAveragingOperator(String name, DAG dag)
  {
    MachineInfoAveragingOperator oper = dag.addOperator(name, MachineInfoAveragingOperator.class);
    dag.setAttribute(oper, Context.OperatorContext.APPLICATION_WINDOW_COUNT, appWindowCountMinute);
    return oper;
  }

  /**
   * This method returns new RedisOutputOperator Operator
   *
   * @param name     the name of the operator in DAG
   * @param dag      the DAG instance
   * @param conf     the configuration object
   * @param database the database instance id
   * @return RedisOutputOperator
   */
  private RedisKeyValPairOutputOperator<MachineKey, Map<String, String>> getRedisOutputOperator(String name, DAG dag, Configuration conf, int database)
  {
    RedisKeyValPairOutputOperator<MachineKey, Map<String, String>> oper = dag.addOperator(name, new RedisKeyValPairOutputOperator<MachineKey, Map<String, String>>());
    String host = conf.get("machinedata.redis.host", "localhost");
    int port = conf.getInt("machinedata.redis.port", 6379);
    RedisStore store = oper.getStore();
    store.setHost(host);
    store.setPort(port);
    store.setDbIndex(database);
    dag.setAttribute(oper, Context.OperatorContext.APPLICATION_WINDOW_COUNT, appWindowCountMinute);
    return oper;
  }

  /**
   * This method returns new SmtpOutputOperator Operator
   *
   * @param name the name of the operator in DAG
   * @param dag  the DAG instance
   * @param conf the configuration object
   * @return SmtpOutputOperator
   */
  private SmtpOutputOperator getSmtpOutputOperator(String name, DAG dag, Configuration conf)
  {
    SmtpOutputOperator mailOper = new SmtpOutputOperator();

    String from = conf.get("machinedata.smtp.from", "admin@datatorrent.com");
    String recipient = conf.get("machinedata.smtp.recipient", "atul@datatorrent.com");
    String subject = conf.get("machinedata.smtp.subject", "Alert!!!");
    String content = conf.get("machinedata.smtp.content", "{}");
    String host = conf.get("machinedata.smtp.host", "localhost");
    int port = conf.getInt("machinedata.smtp.port", 25);
    boolean useSsl = conf.getBoolean("machinedata.smtp.ssl", false);

    mailOper.setFrom(from);
    mailOper.addRecipient(SmtpOutputOperator.RecipientType.TO, recipient);
    mailOper.setSubject(subject);
    mailOper.setContent(content);
    mailOper.setSmtpHost(host);
    mailOper.setSmtpPort(port);
    // mailOper.setSmtpUserName(userName);
    // mailOper.setSmtpPassword(password);
    mailOper.setUseSsl(useSsl);

    dag.addOperator(name, mailOper);

    return mailOper;

  }

  /**
   * This methods the QUEUE_CAPACITY for the given InputPort
   *
   * @param dag       the DAG instance
   * @param inputPort the InputPort whose QUEUE_CAPACITY has to be updated
   */
  private void setDefaultInputPortQueueCapacity(DAG dag, InputPort inputPort)
  {
    dag.setInputPortAttribute(inputPort, PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
  }

  /**
   * This methods the QUEUE_CAPACITY for the given OutputPort
   *
   * @param dag        the DAG instance
   * @param outputPort the OutputPort whose QUEUE_CAPACITY has to be updated
   */
  private void setDefaultOutputPortQueueCapacity(DAG dag, OutputPort outputPort)
  {
    dag.setOutputPortAttribute(outputPort, PortContext.QUEUE_CAPACITY, QUEUE_CAPACITY);
  }

  /**
   * This function sets up the DAG for calculating the average
   *
   * @param dag  the DAG instance
   * @param conf the configuration instance
   * @return MachineInfoAveragingPrerequisitesOperator
   */
  private MachineInfoAveragingPrerequisitesOperator addAverageCalculation(DAG dag, Configuration conf)
  {
    MachineInfoAveragingPrerequisitesOperator prereqAverageOper = getMachineInfoAveragingPrerequisitesOperator("Aggregator", dag);
    dag.setInputPortAttribute(prereqAverageOper.inputPort, PortContext.PARTITION_PARALLEL, true);
    dag.setAttribute(prereqAverageOper, Context.OperatorContext.APPLICATION_WINDOW_COUNT, appWindowCountMinute);

    setDefaultInputPortQueueCapacity(dag, prereqAverageOper.inputPort);
    setDefaultOutputPortQueueCapacity(dag, prereqAverageOper.outputPort);

    int partitions = conf.getInt(Application.class.getName() + ".averagePartitions", 2);
    MachineInfoAveragingOperator averageOperator = getMachineInfoAveragingOperator("AverageCalculator", dag);
    dag.setAttribute(averageOperator, OperatorContext.INITIAL_PARTITION_COUNT, partitions);
    setDefaultInputPortQueueCapacity(dag, averageOperator.inputPort);
    setDefaultOutputPortQueueCapacity(dag, averageOperator.outputPort);

    RedisKeyValPairOutputOperator<MachineKey, Map<String, String>> redisAvgOperator = getRedisOutputOperator("Persister", dag, conf, conf.getInt("machinedata.redis.db", 5));
    setDefaultInputPortQueueCapacity(dag, redisAvgOperator.input);
    dag.setInputPortAttribute(redisAvgOperator.input, PortContext.PARTITION_PARALLEL, true);
    dag.addStream("Average", averageOperator.outputPort, redisAvgOperator.input);

    SmtpOutputOperator smtpOutputOperator = getSmtpOutputOperator("Alerter", dag, conf);

    dag.addStream("Aggregates", prereqAverageOper.outputPort, averageOperator.inputPort);
    dag.addStream("Alerts", averageOperator.smtpAlert, smtpOutputOperator.input);

    return prereqAverageOper;

  }

  /**
   * Create the DAG
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    int partitions = conf.getInt(Application.class.getName() + ".partitions", 1);
    int unifier_count = conf.getInt(Application.class.getName() + ".unifier_count", 2);

    dag.setAttribute(DAG.APPLICATION_NAME, "MachineDataApplication");
    dag.setAttribute(DAG.DEBUG, false);
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, streamingWindowSizeMilliSeconds);

    InputReceiver randomGen = getRandomInformationTupleGenerator("Receiver", dag);
    setDefaultOutputPortQueueCapacity(dag, randomGen.outputInline);
    dag.setAttribute(randomGen, OperatorContext.INITIAL_PARTITION_COUNT, partitions);

    DimensionGenerator dimensionGenerator = dag.addOperator("DimensionsGenerator", DimensionGenerator.class);
    dag.setAttribute(dimensionGenerator, Context.OperatorContext.APPLICATION_WINDOW_COUNT, appWindowCountMinute);
    setDefaultOutputPortQueueCapacity(dag, dimensionGenerator.outputInline);
    setDefaultOutputPortQueueCapacity(dag, dimensionGenerator.output);
    setDefaultInputPortQueueCapacity(dag, dimensionGenerator.inputPort);
    dag.addStream("Events", randomGen.outputInline, dimensionGenerator.inputPort).setLocality(Locality.CONTAINER_LOCAL);
    dag.setInputPortAttribute(dimensionGenerator.inputPort, PortContext.PARTITION_PARALLEL, true);

    MachineInfoAveragingPrerequisitesOperator prereqAverageOper = addAverageCalculation(dag, conf);
    dag.addStream("DimensionalData", dimensionGenerator.outputInline, prereqAverageOper.inputPort).setLocality(Locality.THREAD_LOCAL);
    dag.setOutputPortAttribute(prereqAverageOper.outputPort, PortContext.UNIFIER_LIMIT, unifier_count);

  }
}
