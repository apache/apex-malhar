/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.goldengate;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;

/**
 * This is a demo app which demonstrates the following functionality.
 *
 *    * Writing a Kafka Handler for Oracle Golden Gate, which can send operations performed
 *      on a database to a Kafka Queue.
 *    * Writing Data Torrent Kafka input and output operators.
 *    * Persisting data to HDFS and monitoring files for changes on HDFS
 *    * Using Data Torrent JMS output operators.
 *    * Writing to a target DataBase through golden gate by sending messages to a JMS queue.
 *
 * <b>Summary:</b> A Golden Gate {@link KafkaHandler} writes insertions events on an example employee table from a source database
 * to a Kafka queue. A Data Torrent operator then reads from the Kafka queue to retrieve the insertion events.
 * These insertion events are passed to a Data Torrent CSV operator, which writes events to a CSV file on HDFS.
 * Another operator tails the CSV file to retrieve recent insertion events. Insertion events are then sent to a JMS output
 * operator. Golden Gate then reads from a JMS queue and replays the insertion events on a target database.
 */
@ApplicationAnnotation(name="GoldenGateDemo")
public class GoldenGateApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //////////// Application DAG /////////////////////

    //=====================
    //  Golden Gate Side
    //=====================
    //  Golden Gate Output
    //   _________________________      ___________
    //  |Golden Gate Kafka Handler|--->|Kafka Queue|
    //   -------------------------      -----------
    //  Golden Gate Input
    //   _________      _____________________
    //  |JMS Queue|--->|Golden Gate JMS Input|
    //   ---------      ---------------------
    //=====================
    //  Data Torrent Side
    //=====================
    //   ____________      ____________________      __________
    //  |Kafka Queue |--->|Kafka Input Operator|--->|CSV Writer|__
    //   ------------      --------------------      ----------   |
    //   _________________________________________________________|
    //  |
    //  V_______________      ___________________        _________
    //  |CSV Tail Reader|--->|JMS Output Operator|----> |JMS Queue|
    //   ---------------      -------------------        ---------

    //This operator which receives insertion events from Golden Gate Via Kafka.
    KafkaInput kafkaInput = dag.addOperator("GoldenGateIngest", KafkaInput.class);
    //This operator persists insertion events received via kafka to a CSV file.
    CSVFileOutput csvFileOutput = dag.addOperator("HDFSWriter", CSVFileOutput.class);
    //This operator tails the same CSV to get recent insertion events and forwards these
    //Insertion events to the Golden Gate writer.
    CSVTransactionInput csvFileReader = dag.addOperator("HDFSIncrementalReader", CSVTransactionInput.class);
    //This operator receives insertion events and persists them to a database by sending
    //A JMS message describing the insertion event. Golden Gate then reads the message from the
    //message queue and replays the insertion event on another database.
    GoldenGateJMSOutputOperator jms = dag.addOperator("GoldenGateDestinationTableWriter", GoldenGateJMSOutputOperator.class);

    //Connecting the operator receiving Kafka events to the operator which persists insertion event
    //to a CSV file.
    dag.addStream("InsertedData", kafkaInput.outputPort, csvFileOutput.input);
    //Connection the operator tailing the CSV file to the operator which sends JMS messages to Golden Gate.
    dag.addStream("CSVLines", csvFileReader.outputPort, jms.inputPort);

    //////////// Streamlets ////////////////////////////////////

    //This part of the Application contains Streamlets. Streamlets are a way to request and receive
    //data from a Datatorrent application. The way they work is the following.

    //   ____________          ___________           ____________________
    //  |Data request|------> |Kafka Queue|-------> |Kafka Input Operator|----
    //   ------------          -----------           --------------------    |
    //   ____________________________________________________________________|
    //  |
    //  V_______________________          _____________________          ___________
    //  |Request Handle Operator|------> |Kafka Output Operator|------> |Kafka Queue|----
    //   -----------------------          ---------------------          -----------    |
    //   _______________________________________________________________________________|
    //  |
    //  V______________
    //  |Request Result|
    //   --------------

    //////////// Source Database Streamlet /////////////////////

    //This Streamlet handles reading the latest entries in the source database.

    //This operator receives requests to read from the source database from kafka.
    KafkaSinglePortStringInputOperator dbQueryInput = dag.addOperator("UIQuerySourceOracleTable", KafkaSinglePortStringInputOperator.class);
    //This operator processes requests and reads the latest value from a source database.
    DBQueryProcessor dbQueryProcessor = dag.addOperator("OracleSourceTableRead", DBQueryProcessor.class);
    //This operator responds to requests by sending a message to a kafka topic.
    KafkaSinglePortOutputOperator<Object, Object> dbQueryOutput = dag.addOperator("UI-Display2", new KafkaSinglePortOutputOperator<Object, Object>());

    Properties configProperties = new Properties();
    configProperties.setProperty("serializer.class", KafkaJsonEncoder.class.getName());
    configProperties.setProperty("metadata.broker.list", "node25.morado.com:9092");
    dbQueryOutput.setConfigProperties(configProperties);

    dag.addStream("SourceTableQueries", dbQueryInput.outputPort, dbQueryProcessor.queryInput);
    dag.addStream("SourceTableRows", dbQueryProcessor.queryOutput, dbQueryOutput.inputPort);

    //////////// Destination Database Streamlet /////////////////////

    //This operator receives requests to read from the target database from kafka.
    KafkaSinglePortStringInputOperator odbQueryInput = dag.addOperator("UIQueryDestinationTable", KafkaSinglePortStringInputOperator.class);
    //This operator processes requests and reads the latest value from a target database.
    DBQueryProcessor odbQueryProcessor = dag.addOperator("OracleDestinationTableRead", DBQueryProcessor.class);
    //This operator responds to requests by sending a message to a kafka topic.
    KafkaSinglePortOutputOperator<Object, Object> odbQueryOutput = dag.addOperator("UI-Display3", new KafkaSinglePortOutputOperator<Object, Object>());
    odbQueryOutput.setConfigProperties(configProperties);

    dag.addStream("DestinationTableQueries", odbQueryInput.outputPort, odbQueryProcessor.queryInput);
    dag.addStream("DestinationTableRows", odbQueryProcessor.queryOutput, odbQueryOutput.inputPort);

    //////////// CSV File Streamlet /////////////////////

    //This operator receives requests to read from the CSV file from kafka.
    KafkaSinglePortStringInputOperator fileQueryInput = dag.addOperator("UI-QueryFile", KafkaSinglePortStringInputOperator.class);
    //This operator processes requests and reads the latest value from the CSV file.
    FileQueryProcessor fileQueryProcessor = dag.addOperator("HDFSReader", FileQueryProcessor.class);
    //This operator responds to requests by sending a message to a kafka topic.
    KafkaSinglePortOutputOperator<Object, Object> fileQueryOutput = dag.addOperator("UI-Display1", new KafkaSinglePortOutputOperator<Object, Object>());
    fileQueryOutput.setConfigProperties(configProperties);

    dag.addStream("fileQueries", fileQueryInput.outputPort, fileQueryProcessor.queryInput);
    dag.addStream("fileData", fileQueryProcessor.queryOutput, fileQueryOutput.inputPort);
  }
}
