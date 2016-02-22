/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;

import javax.validation.ConstraintViolationException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.LocalMode;
import com.datatorrent.contrib.kafka.KafkaOperatorTestBase;
import com.datatorrent.contrib.kafka.KafkaTestProducer;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;
import com.example.myapexapp.Application;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Test the application in local mode.
 */
public class ApplicationTest
{
  private final KafkaOperatorTestBase kafkaLauncher = new KafkaOperatorTestBase();
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
  private static final String KAFKA_TOPIC = "exactly-once-test";
  private static final String DB_DRIVER = "org.hsqldb.jdbcDriver";
  private static final String DB_URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";
  private static final String TABLE_NAME = "WORDS";

  @Before
  public void beforeTest() throws Exception {
    kafkaLauncher.baseDir = "target/" + this.getClass().getName();
    FileUtils.deleteDirectory(new File(kafkaLauncher.baseDir));
    kafkaLauncher.startZookeeper();
    kafkaLauncher.startKafkaServer();
    kafkaLauncher.createTopic(0, KAFKA_TOPIC);

    // setup hsqldb
    Class.forName(DB_DRIVER).newInstance();

    Connection con = DriverManager.getConnection(DB_URL);
    Statement stmt = con.createStatement();

    String createMetaTable = "CREATE TABLE IF NOT EXISTS " + JdbcTransactionalStore.DEFAULT_META_TABLE + " ( "
        + JdbcTransactionalStore.DEFAULT_APP_ID_COL + " VARCHAR(100) NOT NULL, "
        + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT NOT NULL, "
        + JdbcTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT NOT NULL, "
        + "UNIQUE (" + JdbcTransactionalStore.DEFAULT_APP_ID_COL + ", "
        + JdbcTransactionalStore.DEFAULT_OPERATOR_ID_COL + ", " + JdbcTransactionalStore.DEFAULT_WINDOW_COL + ") "
        + ")";
    stmt.executeUpdate(createMetaTable);

    String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME
        + "(word VARCHAR(255) not NULL, wcount INTEGER, PRIMARY KEY ( word ))";
    stmt.executeUpdate(createTable);

  }

  @After
  public void afterTest() {
    kafkaLauncher.stopKafkaServer();
    kafkaLauncher.stopZookeeper();
  }

  @Test
  public void testApplication() throws Exception {
    try {
      // produce some test data
      KafkaTestProducer p = new KafkaTestProducer(KAFKA_TOPIC);
      String[] words = "count the words from kafka and store them in the db".split("\\s+");
      p.setMessages(Lists.newArrayList(words));
      new Thread(p).start();

      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      conf.set("dt.operator.kafkaInput.prop.topic", KAFKA_TOPIC);
      conf.set("dt.operator.kafkaInput.prop.zookeeper", "localhost:" + KafkaOperatorTestBase.TEST_ZOOKEEPER_PORT[0]);
      conf.set("dt.operator.kafkaInput.prop.maxTuplesPerWindow", "1"); // consume one word per window
      conf.set("dt.operator.store.prop.store.databaseDriver", DB_DRIVER);
      conf.set("dt.operator.store.prop.store.databaseUrl", DB_URL);

      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync(); // test will terminate after results are available

      HashSet<String> wordsSet = Sets.newHashSet(words);
      Connection con = DriverManager.getConnection(DB_URL);
      Statement stmt = con.createStatement();
      int rowCount = 0;
      long timeout = System.currentTimeMillis() + 30000; // 30s timeout
      while (rowCount < wordsSet.size() && timeout > System.currentTimeMillis()) {
        Thread.sleep(1000);
        String countQuery = "SELECT count(*) from " + TABLE_NAME;
        ResultSet resultSet = stmt.executeQuery(countQuery);
        resultSet.next();
        rowCount = resultSet.getInt(1);
        resultSet.close();
        LOG.info("current row count in {} is {}", TABLE_NAME, rowCount);
      }
      Assert.assertEquals("number of words", wordsSet.size(), rowCount);

      lc.shutdown();

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
