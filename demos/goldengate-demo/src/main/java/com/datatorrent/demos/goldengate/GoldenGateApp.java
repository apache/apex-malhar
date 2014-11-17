/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.goldengate;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="GoldenGateDemo")
public class GoldenGateApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaInput kafkaInput = dag.addOperator("GoldenGateIngest", KafkaInput.class);
    CSVFileOutput csvFileOutput = dag.addOperator("HDFSWriter", CSVFileOutput.class);
    CSVTransactionInput csvFileReader = dag.addOperator("HDFSIncrementalReader", CSVTransactionInput.class);
    //ConsoleOutputOperator console = dag.addOperator("Debug", ConsoleOutputOperator.class);
    GoldenGateJMSOutputOperator jms = dag.addOperator("GoldenGateDestinationTableWriter", GoldenGateJMSOutputOperator.class);

    dag.addStream("InsertedData", kafkaInput.outputPort, csvFileOutput.input);
    //dag.addStream("GoldenGateWriterStream", kafkaInput.transactionPort, jms.inputPort);
    //dag.addStream("csvLines", csvFileReader.outputPort, console.input);
    dag.addStream("CSVLines", csvFileReader.outputPort, jms.inputPort);

    ////

    KafkaSinglePortStringInputOperator dbQueryInput = dag.addOperator("UIQuerySourceOracleTable", KafkaSinglePortStringInputOperator.class);
    DBQueryProcessor dbQueryProcessor = dag.addOperator("OracleSourceTableRead", DBQueryProcessor.class);
    KafkaSinglePortOutputOperator<Object, Object> dbQueryOutput = dag.addOperator("UI-Display2", new KafkaSinglePortOutputOperator<Object, Object>());

    KafkaSinglePortStringInputOperator odbQueryInput = dag.addOperator("UIQueryDestinationTable", KafkaSinglePortStringInputOperator.class);
    DBQueryProcessor odbQueryProcessor = dag.addOperator("OracleDestinationTableRead", DBQueryProcessor.class);
    KafkaSinglePortOutputOperator<Object, Object> odbQueryOutput = dag.addOperator("UI-Display3", new KafkaSinglePortOutputOperator<Object, Object>());

    Properties configProperties = new Properties();
    configProperties.setProperty("serializer.class", KafkaJsonEncoder.class.getName());
    configProperties.setProperty("metadata.broker.list", "node25.morado.com:9092");
    dbQueryOutput.setConfigProperties(configProperties);
    odbQueryOutput.setConfigProperties(configProperties);

    dag.addStream("SourceTableQueries", dbQueryInput.outputPort, dbQueryProcessor.queryInput);
    dag.addStream("SourceTableRows", dbQueryProcessor.queryOutput, dbQueryOutput.inputPort);

    dag.addStream("DestinationTableQueries", odbQueryInput.outputPort, odbQueryProcessor.queryInput);
    dag.addStream("DestinationTableRows", odbQueryProcessor.queryOutput, odbQueryOutput.inputPort);

    ////

    KafkaSinglePortStringInputOperator fileQueryInput = dag.addOperator("UI-QueryFile", KafkaSinglePortStringInputOperator.class);
    FileQueryProcessor fileQueryProcessor = dag.addOperator("HDFSReader", FileQueryProcessor.class);
    KafkaSinglePortOutputOperator<Object, Object> fileQueryOutput = dag.addOperator("UI-Display1", new KafkaSinglePortOutputOperator<Object, Object>());
    fileQueryOutput.setConfigProperties(configProperties);

    //fileQueryOutput.setConfigProperties(configProperties);

    dag.addStream("fileQueries", fileQueryInput.outputPort, fileQueryProcessor.queryInput);
    dag.addStream("fileData", fileQueryProcessor.queryOutput, fileQueryOutput.inputPort);
  }
}
