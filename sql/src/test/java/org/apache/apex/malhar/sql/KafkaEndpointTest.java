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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.apex.malhar.contrib.formatter.CsvFormatter;
import org.apache.apex.malhar.contrib.parser.CsvParser;
import org.apache.apex.malhar.kafka.EmbeddedKafka;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortOutputOperator;
import org.apache.apex.malhar.sql.table.CSVMessageFormat;
import org.apache.apex.malhar.sql.table.KafkaEndpoint;
import org.apache.apex.malhar.sql.table.StreamEndpoint;

import org.apache.hadoop.conf.Configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

public class KafkaEndpointTest
{
  private final String testTopicData0 = "dataTopic0";
  private final String testTopicData1 = "dataTopic1";
  private final String testTopicResult = "resultTopic";

  private EmbeddedKafka kafka;

  private TimeZone defaultTZ;

  @Before
  public void setup() throws IOException
  {
    defaultTZ = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

    kafka = new EmbeddedKafka();
    kafka.start();
    kafka.createTopic(testTopicData0);
    kafka.createTopic(testTopicData1);
    kafka.createTopic(testTopicResult);
  }

  @After
  public void tearDown() throws IOException
  {
    kafka.stop();

    TimeZone.setDefault(defaultTZ);
  }

  @Test
  public void testApplicationSelectInsertWithAPI() throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new KafkaApplication(kafka.getBroker(), testTopicData0, testTopicResult), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      kafka.publish(testTopicData0, Arrays.asList("15/02/2016 10:15:00 +0000,1,paint1,11",
          "15/02/2016 10:16:00 +0000,2,paint2,12",
          "15/02/2016 10:17:00 +0000,3,paint3,13", "15/02/2016 10:18:00 +0000,4,paint4,14",
          "15/02/2016 10:19:00 +0000,5,paint5,15", "15/02/2016 10:10:00 +0000,6,abcde6,16"));

      // TODO: Workaround to add \r\n char to test results because of bug in CsvFormatter which adds new line char.
      String[] expectedLines = new String[]{"15/02/2016 10:18:00 +0000,15/02/2016 00:00:00 +0000,OILPAINT4\r\n",
          "15/02/2016 10:19:00 +0000,15/02/2016 00:00:00 +0000,OILPAINT5\r\n"};

      List<String> consume = kafka.consume(testTopicResult, 30000);
      Assert.assertTrue(Arrays.deepEquals(consume.toArray(new String[consume.size()]), expectedLines));

      lc.shutdown();
    } catch (Exception e) {
      Assert.fail("constraint violations: " + e);
    }
  }

  @Test
  public void testApplicationWithPortEndpoint() throws Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new KafkaPortApplication(kafka.getBroker(), testTopicData0, testTopicResult), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      kafka.publish(testTopicData0, Arrays.asList("15/02/2016 10:15:00 +0000,1,paint1,11",
          "15/02/2016 10:16:00 +0000,2,paint2,12",
          "15/02/2016 10:17:00 +0000,3,paint3,13", "15/02/2016 10:18:00 +0000,4,paint4,14",
          "15/02/2016 10:19:00 +0000,5,paint5,15", "15/02/2016 10:10:00 +0000,6,abcde6,16"));

      // TODO: Workaround to add \r\n char to test results because of bug in CsvFormatter which adds new line char.
      String[] expectedLines = new String[]{"15/02/2016 10:18:00 +0000,15/02/2016 00:00:00 +0000,OILPAINT4\r\n",
          "15/02/2016 10:19:00 +0000,15/02/2016 00:00:00 +0000,OILPAINT5\r\n"};

      List<String> consume = kafka.consume(testTopicResult, 30000);

      Assert.assertTrue(Arrays.deepEquals(consume.toArray(new String[consume.size()]), expectedLines));

      lc.shutdown();
    } catch (Exception e) {
      Assert.fail("constraint violations: " + e);
    }
  }

  @Ignore("Skipping because POJOInnerJoinOperator has issues and needs to be replaced with Windowed variant first.")
  @Test
  public void testApplicationJoin() throws Exception
  {
    String sql = "INSERT INTO SALES " +
        "SELECT STREAM A.ROWTIME, FLOOR(A.ROWTIME TO DAY), " +
        "APEXCONCAT('OILPAINT', SUBSTRING(A.PRODUCT, 6, 7)), B.CATEGORY " +
        "FROM ORDERS AS A " +
        "JOIN CATEGORY AS B ON A.id = B.id " +
        "WHERE A.id > 3 AND A.PRODUCT LIKE 'paint%'";
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new KafkaJoinApplication(kafka.getBroker(), testTopicData0, testTopicData1,
          testTopicResult, sql), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      kafka.publish(testTopicData0, Arrays.asList("15/02/2016 10:15:00 +0000,1,paint1,11",
          "15/02/2016 10:16:00 +0000,2,paint2,12",
          "15/02/2016 10:17:00 +0000,3,paint3,13", "15/02/2016 10:18:00 +0000,4,paint4,14",
          "15/02/2016 10:19:00 +0000,5,paint5,15", "15/02/2016 10:10:00 +0000,6,abcde6,16"));

      kafka.publish(testTopicData1, Arrays.asList("1,ABC",
          "2,DEF",
          "3,GHI", "4,JKL",
          "5,MNO", "6,PQR"));

      // TODO: Workaround to add \r\n char to test results because of bug in CsvFormatter which adds new line char.
      String[] expectedLines = new String[]{"15/02/2016 10:18:00 +0000,15/02/2016 00:00:00 +0000,OILPAINT4,JKL\r\n",
          "15/02/2016 10:19:00 +0000,15/02/2016 00:00:00 +0000,OILPAINT5,MNO\r\n"};

      List<String> consume = kafka.consume(testTopicResult, 30000);

      Assert.assertTrue(Arrays.deepEquals(consume.toArray(new String[consume.size()]), expectedLines));

      lc.shutdown();
    } catch (Exception e) {
      Assert.fail("constraint violations: " + e);
    }
  }

  @Ignore("Skipping because POJOInnerJoinOperator has issues and needs to be replaced with Windowed variant first.")
  @Test
  public void testApplicationJoinFilter() throws Exception
  {
    String sql = "INSERT INTO SALES SELECT STREAM A.ROWTIME, FLOOR(A.ROWTIME TO DAY), " +
        "APEXCONCAT('OILPAINT', SUBSTRING(A.PRODUCT, 6, 7)), B.CATEGORY " +
        "FROM ORDERS AS A JOIN CATEGORY AS B ON A.id = B.id AND A.id > 3" +
        "WHERE A.PRODUCT LIKE 'paint%'";

    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new KafkaJoinApplication(kafka.getBroker(), testTopicData0, testTopicData1,
          testTopicResult, sql), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();

      kafka.publish(testTopicData0, Arrays.asList("15/02/2016 10:15:00 +0000,1,paint1,11",
          "15/02/2016 10:16:00 +0000,2,paint2,12",
          "15/02/2016 10:17:00 +0000,3,paint3,13", "15/02/2016 10:18:00 +0000,4,paint4,14",
          "15/02/2016 10:19:00 +0000,5,paint5,15", "15/02/2016 10:10:00 +0000,6,abcde6,16"));

      kafka.publish(testTopicData1, Arrays.asList("1,ABC",
          "2,DEF",
          "3,GHI", "4,JKL",
          "5,MNO", "6,PQR"));

      // TODO: Workaround to add \r\n char to test results because of bug in CsvFormatter which adds new line char.
      String[] expectedLines = new String[]{"15/02/2016 10:18:00 +0000,15/02/2016 00:00:00 +0000,OILPAINT4,JKL\r\n",
          "15/02/2016 10:19:00 +0000,15/02/2016 00:00:00 +0000,OILPAINT5,MNO\r\n"};

      List<String> consume = kafka.consume(testTopicResult, 30000);

      Assert.assertTrue(Arrays.deepEquals(consume.toArray(new String[consume.size()]), expectedLines));

      lc.shutdown();
    } catch (Exception e) {
      Assert.fail("constraint violations: " + e);
    }
  }

  public static class KafkaApplication implements StreamingApplication
  {
    private String broker;
    private String sourceTopic;
    private String destTopic;

    public KafkaApplication(String broker, String sourceTopic, String destTopic)
    {
      this.broker = broker;
      this.sourceTopic = sourceTopic;
      this.destTopic = destTopic;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      String schemaIn = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
          "{\"name\":\"RowTime\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy HH:mm:ss Z\"}}," +
          "{\"name\":\"id\",\"type\":\"Integer\"}," +
          "{\"name\":\"Product\",\"type\":\"String\"}," +
          "{\"name\":\"units\",\"type\":\"Integer\"}]}";
      String schemaOut = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
          "{\"name\":\"RowTime1\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy HH:mm:ss Z\"}}," +
          "{\"name\":\"RowTime2\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy HH:mm:ss Z\"}}," +
          "{\"name\":\"Product\",\"type\":\"String\"}]}";

      SQLExecEnvironment.getEnvironment()
          .registerTable("ORDERS", new KafkaEndpoint(broker, sourceTopic, new CSVMessageFormat(schemaIn)))
          .registerTable("SALES", new KafkaEndpoint(broker, destTopic, new CSVMessageFormat(schemaOut)))
          .registerFunction("APEXCONCAT", FileEndpointTest.class, "apex_concat_str")
          .executeSQL(dag, "INSERT INTO SALES " + "SELECT STREAM ROWTIME, " + "FLOOR(ROWTIME TO DAY), " +
          "APEXCONCAT('OILPAINT', SUBSTRING(PRODUCT, 6, 7)) " + "FROM ORDERS WHERE ID > 3 " + "AND " +
          "PRODUCT LIKE 'paint%'");
    }
  }

  public static class KafkaJoinApplication implements StreamingApplication
  {
    private String broker;
    private String sourceTopic0;
    private String sourceTopic1;
    private String destTopic;
    private String sql;

    public KafkaJoinApplication(String broker, String sourceTopic0, String sourceTopic1, String destTopic, String sql)
    {
      this.broker = broker;
      this.sourceTopic0 = sourceTopic0;
      this.sourceTopic1 = sourceTopic1;
      this.destTopic = destTopic;
      this.sql = sql;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      String schemaIn0 = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
          "{\"name\":\"RowTime\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy HH:mm:ss Z\"}}," +
          "{\"name\":\"id\",\"type\":\"Integer\"}," +
          "{\"name\":\"Product\",\"type\":\"String\"}," +
          "{\"name\":\"units\",\"type\":\"Integer\"}]}";
      String schemaIn1 = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
          "{\"name\":\"id\",\"type\":\"Integer\"}," +
          "{\"name\":\"Category\",\"type\":\"String\"}]}";
      String schemaOut = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
          "{\"name\":\"RowTime1\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy HH:mm:ss Z\"}}," +
          "{\"name\":\"RowTime2\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy HH:mm:ss Z\"}}," +
          "{\"name\":\"Product\",\"type\":\"String\"}," +
          "{\"name\":\"Category\",\"type\":\"String\"}]}";

      SQLExecEnvironment.getEnvironment()
          .registerTable("ORDERS", new KafkaEndpoint(broker, sourceTopic0, new CSVMessageFormat(schemaIn0)))
          .registerTable("CATEGORY", new KafkaEndpoint(broker, sourceTopic1, new CSVMessageFormat(schemaIn1)))
          .registerTable("SALES", new KafkaEndpoint(broker, destTopic, new CSVMessageFormat(schemaOut)))
          .registerFunction("APEXCONCAT", FileEndpointTest.class, "apex_concat_str")
          .executeSQL(dag, sql);
    }
  }

  public static class KafkaPortApplication implements StreamingApplication
  {
    private String broker;
    private String sourceTopic;
    private String destTopic;

    public KafkaPortApplication(String broker, String sourceTopic, String destTopic)
    {
      this.broker = broker;
      this.sourceTopic = sourceTopic;
      this.destTopic = destTopic;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      String schemaIn = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
          "{\"name\":\"RowTime\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy HH:mm:ss Z\"}}," +
          "{\"name\":\"id\",\"type\":\"Integer\"}," +
          "{\"name\":\"Product\",\"type\":\"String\"}," +
          "{\"name\":\"units\",\"type\":\"Integer\"}]}";
      String schemaOut = "{\"separator\":\",\",\"quoteChar\":\"\\\"\",\"fields\":[" +
          "{\"name\":\"RowTime1\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy HH:mm:ss Z\"}}," +
          "{\"name\":\"RowTime2\",\"type\":\"Date\",\"constraints\":{\"format\":\"dd/MM/yyyy HH:mm:ss Z\"}}," +
          "{\"name\":\"Product\",\"type\":\"String\"}]}";

      KafkaSinglePortInputOperator kafkaInput = dag.addOperator("KafkaInput", KafkaSinglePortInputOperator.class);
      kafkaInput.setTopics(sourceTopic);
      kafkaInput.setInitialOffset("EARLIEST");
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaEndpoint.KEY_DESERIALIZER);
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaEndpoint.VALUE_DESERIALIZER);
      kafkaInput.setConsumerProps(props);
      kafkaInput.setClusters(broker);

      CsvParser csvParser = dag.addOperator("CSVParser", CsvParser.class);
      csvParser.setSchema(schemaIn);

      dag.addStream("KafkaToCSV", kafkaInput.outputPort, csvParser.in);

      CsvFormatter formatter = dag.addOperator("CSVFormatter", CsvFormatter.class);
      formatter.setSchema(schemaOut);

      KafkaSinglePortOutputOperator kafkaOutput = dag.addOperator("KafkaOutput", KafkaSinglePortOutputOperator.class);
      kafkaOutput.setTopic(destTopic);

      props = new Properties();
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaEndpoint.VALUE_SERIALIZER);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaEndpoint.KEY_SERIALIZER);
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
      kafkaOutput.setProperties(props);

      dag.addStream("CSVToKafka", formatter.out, kafkaOutput.inputPort);

      SQLExecEnvironment.getEnvironment()
          .registerTable("ORDERS", new StreamEndpoint(csvParser.out, InputPOJO.class))
          .registerTable("SALES", new StreamEndpoint(formatter.in, OutputPOJO.class))
          .registerFunction("APEXCONCAT", FileEndpointTest.class, "apex_concat_str")
          .executeSQL(dag, "INSERT INTO SALES " + "SELECT STREAM ROWTIME, " + "FLOOR(ROWTIME TO DAY), " +
          "APEXCONCAT('OILPAINT', SUBSTRING(PRODUCT, 6, 7)) " + "FROM ORDERS WHERE ID > 3 " + "AND " +
          "PRODUCT LIKE 'paint%'");
    }
  }
}
