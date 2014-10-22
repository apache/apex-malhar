/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.app;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.goldengate.GoldenGateQueryProcessor;
import com.datatorrent.contrib.goldengate.KafkaJsonEncoder;
import com.datatorrent.contrib.goldengate.lib.*;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="GoldenGateDemo")
public class GoldenGateApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaInput kafkaInput = new KafkaInput();
    dag.addOperator("kafkaInput", kafkaInput);

    ////

    OracleDBOutputOperator db = new OracleDBOutputOperator();
    dag.addOperator("oracledb", db);

    ////

    ConsoleOutputOperator console = new ConsoleOutputOperator();
    dag.addOperator("console", console);

    ////

    CSVFileOutput csvFileOutput = new CSVFileOutput();
    csvFileOutput.setOutputFileName("/user/tim/dtv.csv");
    dag.addOperator("csv", csvFileOutput);

    ////

    dag.addStream("display", kafkaInput.outputPort, console.input);
    dag.addStream("inputtodb", kafkaInput.employeePort, db.input);
    dag.addStream("csvstream", kafkaInput.employeePort, csvFileOutput.input);

    ////

    KafkaSinglePortStringInputOperator queryInput = dag.addOperator("QueryInput", KafkaSinglePortStringInputOperator.class);

    //

    GoldenGateQueryProcessor queryProcessor = dag.addOperator("QueryProcessor", GoldenGateQueryProcessor.class);

    //

    KafkaSinglePortOutputOperator<Object, Object> queryOutput = dag.addOperator("QueryResult", new KafkaSinglePortOutputOperator<Object, Object>());

    Properties configProperties = new Properties();
    configProperties.setProperty("serializer.class", KafkaJsonEncoder.class.getName());
    configProperties.setProperty("metadata.broker.list", "node25.morado.com:9092");
    queryOutput.setConfigProperties(configProperties);

    dag.addStream("queries", queryInput.outputPort, queryProcessor.queryInput);
    dag.addStream("results", queryProcessor.queryOutput, queryOutput.inputPort);
  }
}
