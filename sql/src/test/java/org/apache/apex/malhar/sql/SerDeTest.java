/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.sql;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import org.apache.apex.malhar.contrib.formatter.CsvFormatter;
import org.apache.apex.malhar.contrib.parser.CsvParser;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortOutputOperator;
import org.apache.apex.malhar.sql.table.CSVMessageFormat;
import org.apache.apex.malhar.sql.table.Endpoint;
import org.apache.apex.malhar.sql.table.FileEndpoint;
import org.apache.apex.malhar.sql.table.KafkaEndpoint;
import org.apache.apex.malhar.sql.table.StreamEndpoint;

import org.apache.commons.io.FileUtils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.datatorrent.stram.plan.logical.LogicalPlan;

public class SerDeTest
{
  @Test
  public void testSQLWithApexFactory() throws IOException, ClassNotFoundException
  {
    File modelFile = new File("src/test/resources/model/model_file_csv.json");
    String model = FileUtils.readFileToString(modelFile);

    LogicalPlan dag = new LogicalPlan();
    SQLExecEnvironment.getEnvironment()
        .withModel(model)
        .executeSQL(dag, "SELECT STREAM ROWTIME, PRODUCT FROM ORDERS");

    dag.validate();
  }

  @Test
  public void testSQLWithAPI() throws ClassNotFoundException, IOException
  {
    LogicalPlan dag = new LogicalPlan();

    String schema = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
        "{\"name\":\"RowTime\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy hh:mm:ss Z\"}}," +
        "{\"name\":\"id\",\"type\":\"Integer\"},{\"name\":\"Product\",\"type\":\"String\"}," +
        "{\"name\":\"units\",\"type\":\"Integer\"}]}";

    Endpoint endpoint = new FileEndpoint("dummyFilePath", new CSVMessageFormat(schema));

    SQLExecEnvironment.getEnvironment()
        .registerTable("ORDERS", endpoint)
        .executeSQL(dag, "SELECT STREAM FLOOR(ROWTIME TO HOUR), SUBSTRING(PRODUCT, 0, 5) FROM ORDERS WHERE id > 3");

    dag.validate();
  }

  @Test
  public void testSQLSelectInsertWithAPI() throws IOException, ClassNotFoundException
  {
    LogicalPlan dag = new LogicalPlan();

    String schemaIn = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
        "{\"name\":\"RowTime\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy hh:mm:ss\"}}," +
        "{\"name\":\"id\",\"type\":\"Integer\"}," +
        "{\"name\":\"Product\",\"type\":\"String\"}," +
        "{\"name\":\"units\",\"type\":\"Integer\"}]}";
    String schemaOut = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
        "{\"name\":\"RowTime\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy hh:mm:ss\"}}," +
        "{\"name\":\"Product\",\"type\":\"String\"}]}";

    SQLExecEnvironment.getEnvironment()
        .registerTable("ORDERS", new FileEndpoint("dummyFilePathInput", new CSVMessageFormat(schemaIn)))
        .registerTable("SALES", new FileEndpoint("dummyFilePathOutput", "out.tmp", new CSVMessageFormat(schemaOut)))
        .executeSQL(dag, "INSERT INTO SALES SELECT STREAM FLOOR(ROWTIME TO HOUR), SUBSTRING(PRODUCT, 0, 5) " +
        "FROM ORDERS WHERE id > 3");

    dag.validate();
  }

  @Test
  public void testJoin() throws IOException, ClassNotFoundException
  {
    LogicalPlan dag = new LogicalPlan();
    String schemaIn0 = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
        "{\"name\":\"RowTime\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy hh:mm:ss Z\"}}," +
        "{\"name\":\"id\",\"type\":\"Integer\"}," +
        "{\"name\":\"Product\",\"type\":\"String\"}," +
        "{\"name\":\"units\",\"type\":\"Integer\"}]}";
    String schemaIn1 = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
        "{\"name\":\"id\",\"type\":\"Integer\"}," +
        "{\"name\":\"Category\",\"type\":\"String\"}]}";
    String schemaOut = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
        "{\"name\":\"RowTime1\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy hh:mm:ss Z\"}}," +
        "{\"name\":\"RowTime2\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy hh:mm:ss Z\"}}," +
        "{\"name\":\"Product\",\"type\":\"String\"}," +
        "{\"name\":\"Category\",\"type\":\"String\"}]}";

    String sql = "INSERT INTO SALES SELECT STREAM A.ROWTIME, FLOOR(A.ROWTIME TO DAY), " +
        "APEXCONCAT('OILPAINT', SUBSTRING(A.PRODUCT, 6, 7)), B.CATEGORY " +
        "FROM ORDERS AS A " +
        "JOIN CATEGORY AS B ON A.id = B.id " +
        "WHERE A.id > 3 AND A.PRODUCT LIKE 'paint%'";

    SQLExecEnvironment.getEnvironment()
        .registerTable("ORDERS", new KafkaEndpoint("localhost:9092", "testdata0", new CSVMessageFormat(schemaIn0)))
        .registerTable("CATEGORY", new KafkaEndpoint("localhost:9092", "testdata1", new CSVMessageFormat(schemaIn1)))
        .registerTable("SALES", new KafkaEndpoint("localhost:9092", "testresult", new CSVMessageFormat(schemaOut)))
        .registerFunction("APEXCONCAT", FileEndpointTest.class, "apex_concat_str")
        .executeSQL(dag, sql);

    dag.validate();
  }

  @Test
  public void testJoinFilter() throws IOException, ClassNotFoundException
  {
    LogicalPlan dag = new LogicalPlan();
    String schemaIn0 = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
        "{\"name\":\"RowTime\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy hh:mm:ss Z\"}}," +
        "{\"name\":\"id\",\"type\":\"Integer\"}," +
        "{\"name\":\"Product\",\"type\":\"String\"}," +
        "{\"name\":\"units\",\"type\":\"Integer\"}]}";
    String schemaIn1 = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
        "{\"name\":\"id\",\"type\":\"Integer\"}," +
        "{\"name\":\"Category\",\"type\":\"String\"}]}";
    String schemaOut = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
        "{\"name\":\"RowTime1\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy hh:mm:ss Z\"}}," +
        "{\"name\":\"RowTime2\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy hh:mm:ss Z\"}}," +
        "{\"name\":\"Product\",\"type\":\"String\"}," +
        "{\"name\":\"Category\",\"type\":\"String\"}]}";

    String sql = "INSERT INTO SALES SELECT STREAM A.ROWTIME, FLOOR(A.ROWTIME TO DAY), " +
        "APEXCONCAT('OILPAINT', SUBSTRING(A.PRODUCT, 6, 7)), B.CATEGORY " +
        "FROM ORDERS AS A JOIN CATEGORY AS B ON A.id = B.id AND A.id > 3" +
        "WHERE A.PRODUCT LIKE 'paint%'";

    SQLExecEnvironment.getEnvironment()
        .registerTable("ORDERS", new KafkaEndpoint("localhost:9092", "testdata0", new CSVMessageFormat(schemaIn0)))
        .registerTable("CATEGORY", new KafkaEndpoint("localhost:9092", "testdata1", new CSVMessageFormat(schemaIn1)))
        .registerTable("SALES", new KafkaEndpoint("localhost:9092", "testresult", new CSVMessageFormat(schemaOut)))
        .registerFunction("APEXCONCAT", FileEndpointTest.class, "apex_concat_str")
        .executeSQL(dag, sql);

    dag.validate();
  }

  @Test
  public void testPortEndpoint() throws IOException, ClassNotFoundException
  {
    LogicalPlan dag = new LogicalPlan();

    String schemaIn = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
        "{\"name\":\"RowTime\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy hh:mm:ss Z\"}}," +
        "{\"name\":\"id\",\"type\":\"Integer\"}," +
        "{\"name\":\"Product\",\"type\":\"String\"}," +
        "{\"name\":\"units\",\"type\":\"Integer\"}]}";
    String schemaOut = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
        "{\"name\":\"RowTime1\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy hh:mm:ss Z\"}}," +
        "{\"name\":\"RowTime2\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy hh:mm:ss Z\"}}," +
        "{\"name\":\"Product\",\"type\":\"String\"}]}";

    KafkaSinglePortInputOperator kafkaInput = dag.addOperator("KafkaInput", KafkaSinglePortInputOperator.class);
    kafkaInput.setTopics("testdata0");
    kafkaInput.setInitialOffset("EARLIEST");
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaEndpoint.KEY_DESERIALIZER);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaEndpoint.VALUE_DESERIALIZER);
    kafkaInput.setConsumerProps(props);
    kafkaInput.setClusters("localhost:9092");

    CsvParser csvParser = dag.addOperator("CSVParser", CsvParser.class);
    csvParser.setSchema(schemaIn);

    dag.addStream("KafkaToCSV", kafkaInput.outputPort, csvParser.in);

    CsvFormatter formatter = dag.addOperator("CSVFormatter", CsvFormatter.class);
    formatter.setSchema(schemaOut);

    KafkaSinglePortOutputOperator kafkaOutput = dag.addOperator("KafkaOutput", KafkaSinglePortOutputOperator.class);
    kafkaOutput.setTopic("testresult");

    props = new Properties();
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaEndpoint.VALUE_SERIALIZER);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaEndpoint.KEY_SERIALIZER);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    kafkaOutput.setProperties(props);

    dag.addStream("CSVToKafka", formatter.out, kafkaOutput.inputPort);

    SQLExecEnvironment.getEnvironment()
        .registerTable("ORDERS", new StreamEndpoint(csvParser.out, InputPOJO.class))
        .registerTable("SALES", new StreamEndpoint(formatter.in, OutputPOJO.class))
        .registerFunction("APEXCONCAT", FileEndpointTest.class, "apex_concat_str")
        .executeSQL(dag, "INSERT INTO SALES " + "SELECT STREAM ROWTIME, " + "FLOOR(ROWTIME TO DAY), " +
        "APEXCONCAT('OILPAINT', SUBSTRING(PRODUCT, 6, 7)) " + "FROM ORDERS WHERE ID > 3 " + "AND " +
        "PRODUCT LIKE 'paint%'");

    dag.validate();
  }
}
