/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.goldengate.app;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.io.ConsoleOutputOperator;

import com.datatorrent.contrib.goldengate.DBQueryProcessor;
import com.datatorrent.contrib.goldengate.KafkaJsonEncoder;
import com.datatorrent.contrib.goldengate.lib.CSVFileOutput;
import com.datatorrent.contrib.goldengate.lib.KafkaInput;
import com.datatorrent.contrib.goldengate.lib.OracleDBOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

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
    csvFileOutput.setFilePath("/user/tim");
    csvFileOutput.setOutputFileName("dtv.csv");
    dag.addOperator("csv", csvFileOutput);

    ////

    dag.addStream("display", kafkaInput.outputPort, console.input);
    dag.addStream("inputtodb", kafkaInput.employeePort, db.input);
    dag.addStream("csvstream", kafkaInput.employeePort1, csvFileOutput.input);

    ////

    KafkaSinglePortStringInputOperator dbQueryInput = dag.addOperator("DBQuery", KafkaSinglePortStringInputOperator.class);
    DBQueryProcessor dbQueryProcessor = dag.addOperator("DBQueryProcessor", DBQueryProcessor.class);
    KafkaSinglePortOutputOperator<Object, Object> dbQueryOutput = dag.addOperator("DBQueryResponse", new KafkaSinglePortOutputOperator<Object, Object>());

    Properties configProperties = new Properties();
    configProperties.setProperty("serializer.class", KafkaJsonEncoder.class.getName());
    configProperties.setProperty("metadata.broker.list", "node25.morado.com:9092");
    dbQueryOutput.setConfigProperties(configProperties);

    dag.addStream("dbQueries", dbQueryInput.outputPort, dbQueryProcessor.queryInput);
    dag.addStream("dbRows", dbQueryProcessor.queryOutput, dbQueryOutput.inputPort);

    KafkaSinglePortStringInputOperator fileQueryInput = dag.addOperator("FileQuery", KafkaSinglePortStringInputOperator.class);
    DBQueryProcessor fileQueryProcessor = dag.addOperator("FileQueryProcessor", DBQueryProcessor.class);
    KafkaSinglePortOutputOperator<Object, Object> fileQueryOutput = dag.addOperator("FileQueryResponse", new KafkaSinglePortOutputOperator<Object, Object>());

    fileQueryOutput.setConfigProperties(configProperties);

    dag.addStream("fileQueries", fileQueryInput.outputPort, fileQueryProcessor.queryInput);
    dag.addStream("fileData", fileQueryProcessor.queryOutput, fileQueryOutput.inputPort);
  }
}
