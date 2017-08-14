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
package org.apache.apex.examples.exactlyonce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.EmbeddedAppLauncher;
import org.apache.apex.api.Launcher;
import org.apache.apex.api.Launcher.AppHandle;
import org.apache.apex.api.Launcher.LaunchMode;
import org.apache.apex.api.Launcher.ShutdownMode;
import org.apache.apex.malhar.lib.db.jdbc.JdbcTransactionalStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;

import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;

import info.batey.kafka.unit.KafkaUnit;
import info.batey.kafka.unit.KafkaUnitRule;
import kafka.producer.KeyedMessage;

public class ExactlyOnceJdbcOutputTest
{
  private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceJdbcOutputTest.class);
  private static final String DB_DRIVER = "org.hsqldb.jdbcDriver";
  private static final String DB_URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";
  private static final String TABLE_NAME = "WORDS";

  private final int brokerPort = NetUtils.getFreeSocketPort();
  private final int zkPort = NetUtils.getFreeSocketPort();

  @Rule
  public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(zkPort, brokerPort);

  {
    // required to avoid 50 partitions auto creation
    this.kafkaUnitRule.getKafkaUnit().setKafkaBrokerConfig("num.partitions", "1");
    this.kafkaUnitRule.getKafkaUnit().setKafkaBrokerConfig("offsets.topic.num.partitions", "1");
  }

  @Before
  public void beforeTest() throws Exception
  {
    // setup hsqldb
    Class.forName(DB_DRIVER).newInstance();

    Connection con = DriverManager.getConnection(DB_URL);
    Statement stmt = con.createStatement();

    String createMetaTable = "CREATE TABLE IF NOT EXISTS "
        + JdbcTransactionalStore.DEFAULT_META_TABLE + " ( "
        + JdbcTransactionalStore.DEFAULT_APP_ID_COL + " VARCHAR(100) NOT NULL, "
        + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT NOT NULL, "
        + JdbcTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT NOT NULL, "
        + "UNIQUE (" + JdbcTransactionalStore.DEFAULT_APP_ID_COL + ", "
        + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + ", "
        + JdbcTransactionalStore.DEFAULT_WINDOW_COL + ") "
        + ")";
    stmt.executeUpdate(createMetaTable);

    String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME
        + "(word VARCHAR(255) not NULL, wcount INTEGER, PRIMARY KEY ( word ))";
    stmt.executeUpdate(createTable);
  }

  @Test
  public void testApplication() throws Exception
  {
    KafkaUnit ku = kafkaUnitRule.getKafkaUnit();
    String topicName = "testTopic";
    // topic creation is async and the producer may also auto-create it
    ku.createTopic(topicName, 1);

    // produce test data
    String[] words = "count the words from kafka and store them in the db".split("\\s+");
    for (String word : words) {
      ku.sendMessages(new KeyedMessage<String, String>(topicName, word));
    }

    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    conf.set("apex.operator.kafkaInput.prop.topics", topicName);
    conf.set("apex.operator.kafkaInput.prop.clusters", "localhost:" + brokerPort);
    conf.set("apex.operator.kafkaInput.prop.maxTuplesPerWindow", "1"); // consume one word per window
    conf.set("apex.operator.kafkaInput.prop.initialOffset", "EARLIEST");
    conf.set("apex.operator.store.prop.store.databaseDriver", DB_DRIVER);
    conf.set("apex.operator.store.prop.store.databaseUrl", DB_URL);

    EmbeddedAppLauncher<?> launcher = Launcher.getLauncher(LaunchMode.EMBEDDED);
    Attribute.AttributeMap launchAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    launchAttributes.put(EmbeddedAppLauncher.RUN_ASYNC, true); // terminate after results are available
    AppHandle appHandle = launcher.launchApp(new ExactlyOnceJdbcOutputApp(), conf, launchAttributes);
    HashSet<String> wordsSet = Sets.newHashSet(words);
    Connection con = DriverManager.getConnection(DB_URL);
    Statement stmt = con.createStatement();
    int rowCount = 0;
    long timeout = System.currentTimeMillis() + 30000; // 30s timeout
    while (rowCount < wordsSet.size() && timeout > System.currentTimeMillis()) {
      Thread.sleep(500);
      String countQuery = "SELECT count(*) from " + TABLE_NAME;
      ResultSet resultSet = stmt.executeQuery(countQuery);
      resultSet.next();
      rowCount = resultSet.getInt(1);
      resultSet.close();
      LOG.info("current row count in {} is {}", TABLE_NAME, rowCount);
    }
    Assert.assertEquals("number of words", wordsSet.size(), rowCount);
    appHandle.shutdown(ShutdownMode.KILL);
  }

}
