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
package org.apache.apex.malhar.lib.db.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.TestEvent;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.ProcessingMode;
import com.datatorrent.netlet.util.DTThrowable;

import static org.apache.apex.malhar.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.APP_ID;
import static org.apache.apex.malhar.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.OPERATOR_ID;
import static org.apache.apex.malhar.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.TABLE_NAME;
import static org.apache.apex.malhar.lib.db.jdbc.JdbcOperatorTest.DB_DRIVER;
import static org.apache.apex.malhar.lib.db.jdbc.JdbcOperatorTest.URL;
import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

/**
 * Test for {@link AbstractJdbcNonTransactionableBatchOutputOperator}
 */
public class JdbcNonTransactionalBatchOutputOperatorTest
{
  private static final Logger LOG = LoggerFactory.getLogger(JdbcNonTransactionalBatchOutputOperatorTest.class);

  public static final int HALF_BATCH_SIZE = 5;
  public static final int BATCH_SIZE = HALF_BATCH_SIZE * 2;

  private static Connection con;

  @BeforeClass
  public static void setup()
  {
    //Maintain connection to database between tests
    JdbcOperatorTest.setup();

    try {
      Class.forName(DB_DRIVER).newInstance();
      con = DriverManager.getConnection(URL);
    } catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
      DTThrowable.rethrow(ex);
    }
  }

  @AfterClass
  public static void teardown()
  {
    //Close connection to database
    JdbcOperatorTest.setup();

    try {
      con.close();
    } catch (SQLException ex) {
      DTThrowable.rethrow(ex);
    }
  }

  private static class TestOutputOperator extends AbstractJdbcNonTransactionableBatchOutputOperator<TestEvent, JdbcNonTransactionalStore>
  {
    @Override
    protected String getUpdateCommand()
    {
      return JdbcNonTransactionalOutputOperatorTest.TestOutputOperator.INSERT_STMT;
    }

    @Override
    protected void setStatementParameters(PreparedStatement statement, TestEvent tuple) throws SQLException
    {
      statement.setInt(1, tuple.id);
    }

    public int getNumOfEventsInStore(Connection con)
    {
      try {
        Statement stmt = con.createStatement();

        String countQuery = "SELECT count(*) FROM " + TABLE_NAME;
        ResultSet resultSet = stmt.executeQuery(countQuery);
        resultSet.next();
        int count = resultSet.getInt(1);
        stmt.close();
        return count;
      } catch (SQLException e) {
        throw new RuntimeException("fetching count", e);
      }
    }
  }

  private static TestOutputOperator createOperator(ProcessingMode processingMode)
  {
    JdbcNonTransactionalStore store = new JdbcNonTransactionalStore();
    store.setDatabaseDriver(JdbcNonTransactionalOutputOperatorTest.DB_DRIVER);
    store.setDatabaseUrl(JdbcNonTransactionalOutputOperatorTest.URL);

    TestOutputOperator outputOperator = new TestOutputOperator();

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, processingMode);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);
    outputOperator.setStore(store);
    outputOperator.setBatchSize(BATCH_SIZE);

    outputOperator.setup(context);

    return outputOperator;
  }

  @Test
  public void testBatch()
  {
    JdbcOperatorTest.cleanTable();
    Random random = new Random();
    TestOutputOperator outputOperator = createOperator(ProcessingMode.AT_LEAST_ONCE);

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();

    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.beginWindow(1);

    for (int batchCounter = 0; batchCounter < HALF_BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();

    Assert.assertEquals("Commit window id ", 1, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should not be written", BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.beginWindow(2);

    for (int batchCounter = 0; batchCounter < HALF_BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();


    Assert.assertEquals("Commit window id ", 2, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should not be written", 2 * BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.teardown();
  }

  @Test
  public void testAtLeastOnceFullBatch()
  {
    JdbcOperatorTest.cleanTable();
    Random random = new Random();
    TestOutputOperator outputOperator = createOperator(ProcessingMode.AT_LEAST_ONCE);

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();


    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.beginWindow(1);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", 2 * BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.getStore().disconnect();

    ////

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, 0L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);
    outputOperator.setup(context);

    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", 2 * BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();

    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", 2 * BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.beginWindow(1);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();

    Assert.assertEquals("Commit window id ", 1, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", 3 * BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));
  }

  @Test
  public void testAtLeastOnceHalfBatch()
  {
    JdbcOperatorTest.cleanTable();
    Random random = new Random();
    TestOutputOperator outputOperator = createOperator(ProcessingMode.AT_LEAST_ONCE);

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();


    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.beginWindow(1);

    for (int batchCounter = 0; batchCounter < HALF_BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.getStore().disconnect();

    ////

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, 0L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    outputOperator.setup(context);

    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();

    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.beginWindow(1);

    for (int batchCounter = 0; batchCounter < HALF_BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();

    Assert.assertEquals("Commit window id ", 1, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", 2 * BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));
  }

  @Test
  public void testAtMostOnceFullBatch()
  {
    JdbcOperatorTest.cleanTable();
    Random random = new Random();
    TestOutputOperator outputOperator = createOperator(ProcessingMode.AT_MOST_ONCE);

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();

    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.beginWindow(1);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", 2 * BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.getStore().disconnect();

    ////

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_MOST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, 0L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);
    outputOperator.setup(context);

    outputOperator.beginWindow(2);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();

    Assert.assertEquals("Commit window id ", 2, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", 3 * BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));
  }

  @Test
  public void testAtMostOnceHalfBatch()
  {
    JdbcOperatorTest.cleanTable();
    Random random = new Random();
    TestOutputOperator outputOperator = createOperator(ProcessingMode.AT_MOST_ONCE);

    outputOperator.beginWindow(0);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();


    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.beginWindow(1);

    for (int batchCounter = 0; batchCounter < HALF_BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.getStore().disconnect();

    ////

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_MOST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, 0L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);
    outputOperator.setup(context);

    Assert.assertEquals("Commit window id ", 0, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));

    outputOperator.beginWindow(2);

    for (int batchCounter = 0; batchCounter < BATCH_SIZE; batchCounter++) {
      outputOperator.input.put(new TestEvent(random.nextInt()));
    }

    outputOperator.endWindow();

    Assert.assertEquals("Commit window id ", 2, outputOperator.getStore().getCommittedWindowId(APP_ID, OPERATOR_ID));
    Assert.assertEquals("Batch should be written", 2 * BATCH_SIZE,
        outputOperator.getNumOfEventsInStore(outputOperator.getStore().connection));
  }
}
