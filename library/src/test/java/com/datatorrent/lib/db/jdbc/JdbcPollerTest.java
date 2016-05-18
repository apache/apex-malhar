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
package com.datatorrent.lib.db.jdbc;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

/**
 * Tests for {@link AbstractJdbcPollInputOperator} and
 * {@link JdbcPollInputOperator}
 */
public class JdbcPollerTest
{
  public static final String DB_DRIVER = "org.hsqldb.jdbcDriver";
  public static final String URL = "jdbc:hsqldb:mem:test;sql.syntax_mys=true";

  private static final String TABLE_NAME = "test_account_table";
  private static String APP_ID = "JdbcPollingOperatorTest";
  public String dir = null;

  @BeforeClass
  public static void setup()
  {
    try {
      cleanup();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    try {
      Class.forName(DB_DRIVER).newInstance();

      Connection con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();

      String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME
          + " (Account_No INTEGER, Name VARCHAR(255), Amount INTEGER)";
      stmt.executeUpdate(createTable);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AfterClass
  public static void cleanup()
  {
    try {
      FileUtils.deleteDirectory(new File("target/" + APP_ID));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void cleanTable()
  {
    try {
      Connection con = DriverManager.getConnection(URL);
      Statement stmt = con.createStatement();
      String cleanTable = "delete from " + TABLE_NAME;
      stmt.executeUpdate(cleanTable);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static void insertEventsInTable(int numEvents, int offset)
  {
    try {
      Connection con = DriverManager.getConnection(URL);
      String insert = "insert into " + TABLE_NAME + " values (?,?,?)";
      PreparedStatement stmt = con.prepareStatement(insert);
      for (int i = 0; i < numEvents; i++, offset++) {
        stmt.setInt(1, offset);
        stmt.setString(2, "Account_Holder-" + offset);
        stmt.setInt(3, (offset * 1000));
        stmt.executeUpdate();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Simulates actual application flow Adds a batch query partitiom, a pollable
   * partition Incremental record polling is also checked
   */
  @Test
  public void testJdbcPollingInputOperatorBatch() throws InterruptedException
  {
    cleanTable();
    insertEventsInTable(10, 0);
    JdbcStore store = new JdbcStore();
    store.setDatabaseDriver(DB_DRIVER);
    store.setDatabaseUrl(URL);

    Attribute.AttributeMap.DefaultAttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    this.dir = "target/" + APP_ID + "/";
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    attributeMap.put(Context.DAGContext.APPLICATION_PATH, dir);

    JdbcPollInputOperator inputOperator = new JdbcPollInputOperator();
    inputOperator.setStore(store);
    inputOperator.setBatchSize(100);
    inputOperator.setPollInterval(1000);
    inputOperator.setEmitColumnList("Account_No,Name,Amount");
    inputOperator.setKey("Account_No");
    inputOperator.setTableName(TABLE_NAME);
    inputOperator.setFetchSize(100);
    inputOperator.setPartitionCount(1);

    CollectorTestSink<Object> sink = new CollectorTestSink<>();
    inputOperator.outputPort.setSink(sink);

    TestUtils.MockBatchedOperatorStats readerStats = new TestUtils.MockBatchedOperatorStats(2);

    DefaultPartition<AbstractJdbcPollInputOperator<Object>> apartition = new DefaultPartition<AbstractJdbcPollInputOperator<Object>>(
        inputOperator);

    TestUtils.MockPartition<AbstractJdbcPollInputOperator<Object>> pseudoParttion = new TestUtils.MockPartition<>(
        apartition, readerStats);

    List<Partitioner.Partition<AbstractJdbcPollInputOperator<Object>>> newMocks = Lists.newArrayList();

    newMocks.add(pseudoParttion);

    Collection<com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<Object>>> newPartitions = inputOperator
        .definePartitions(newMocks, null);

    Iterator<com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<Object>>> itr = newPartitions
        .iterator();

    int operatorId = 0;
    for (com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<Object>> partition : newPartitions) {

      Attribute.AttributeMap.DefaultAttributeMap partitionAttributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
      this.dir = "target/" + APP_ID + "/";
      partitionAttributeMap.put(DAG.APPLICATION_ID, APP_ID);
      partitionAttributeMap.put(Context.DAGContext.APPLICATION_PATH, dir);

      OperatorContextTestHelper.TestIdOperatorContext partitioningContext = new OperatorContextTestHelper.TestIdOperatorContext(
          operatorId++, partitionAttributeMap);

      partition.getPartitionedInstance().setup(partitioningContext);
      partition.getPartitionedInstance().activate(partitioningContext);
    }

    //First partition is for range queries,last is for polling queries
    AbstractJdbcPollInputOperator<Object> newInstance = itr.next().getPartitionedInstance();
    CollectorTestSink<Object> sink1 = new CollectorTestSink<>();
    newInstance.outputPort.setSink(sink1);
    newInstance.beginWindow(1);
    Thread.sleep(50);
    newInstance.emitTuples();
    newInstance.endWindow();

    Assert.assertEquals("rows from db", 10, sink1.collectedTuples.size());
    int i = 0;
    for (Object tuple : sink1.collectedTuples) {
      String[] pojoEvent = tuple.toString().split(",");
      Assert.assertTrue("i=" + i, Integer.parseInt(pojoEvent[0]) == i ? true : false);
      i++;
    }
    sink1.collectedTuples.clear();

    insertEventsInTable(10, 10);

    AbstractJdbcPollInputOperator<Object> pollableInstance = itr.next().getPartitionedInstance();

    pollableInstance.outputPort.setSink(sink1);

    pollableInstance.beginWindow(1);
    Thread.sleep(pollableInstance.getPollInterval());
    pollableInstance.emitTuples();
    pollableInstance.endWindow();
    

    Assert.assertEquals("rows from db", 10, sink1.collectedTuples.size());
    i = 10;
    for (Object tuple : sink1.collectedTuples) {
      String[] pojoEvent = tuple.toString().split(",");
      Assert.assertTrue("i=" + i, Integer.parseInt(pojoEvent[0]) == i ? true : false);
      i++;
    }

    sink1.collectedTuples.clear();
    insertEventsInTable(10, 20);

    pollableInstance.beginWindow(2);
    Thread.sleep(pollableInstance.getPollInterval());
    pollableInstance.emitTuples();
    pollableInstance.endWindow();
    
    Assert.assertEquals("rows from db", 10, sink1.collectedTuples.size());

    i = 20;
    for (Object tuple : sink1.collectedTuples) {
      String[] pojoEvent = tuple.toString().split(",");
      Assert.assertTrue("i=" + i, Integer.parseInt(pojoEvent[0]) == i ? true : false);
      i++;
    }
    sink1.collectedTuples.clear();
  }

}
