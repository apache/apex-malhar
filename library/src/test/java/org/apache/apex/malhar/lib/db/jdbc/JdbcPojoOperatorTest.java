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
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.helper.TestPortContext;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.TestUtils;

import com.google.common.collect.Lists;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

import com.datatorrent.api.DAG;

/**
 * Tests for {@link AbstractJdbcTransactionableOutputOperator} and
 * {@link AbstractJdbcInputOperator}
 */
public class JdbcPojoOperatorTest extends JdbcOperatorTest
{

  private static class TestOutputOperator extends AbstractJdbcTransactionableOutputOperator<TestEvent>
  {
    private static final String INSERT_STMT = "INSERT INTO " + TABLE_NAME + " values (?)";

    TestOutputOperator()
    {
      cleanTable();
    }

    @Nonnull
    @Override
    protected String getUpdateCommand()
    {
      return INSERT_STMT;
    }

    @Override
    protected void setStatementParameters(PreparedStatement statement, TestEvent tuple) throws SQLException
    {
      statement.setInt(1, tuple.id);
    }

    public int getNumOfEventsInStore()
    {
      Connection con;
      try {
        con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();

        String countQuery = "SELECT count(*) from " + TABLE_NAME;
        ResultSet resultSet = stmt.executeQuery(countQuery);
        resultSet.next();
        return resultSet.getInt(1);
      } catch (SQLException e) {
        throw new RuntimeException("fetching count", e);
      }
    }
  }

  private static class TestInputOperator extends AbstractJdbcInputOperator<TestEvent>
  {

    private static final String retrieveQuery = "SELECT * FROM " + TABLE_NAME;

    TestInputOperator()
    {
      cleanTable();
    }

    @Override
    public TestEvent getTuple(ResultSet result)
    {
      try {
        return new TestEvent(result.getInt(1));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String queryToRetrieveData()
    {
      return retrieveQuery;
    }
  }

  private static class TestPOJOOutputOperator extends JdbcPOJOInsertOutputOperator
  {
    TestPOJOOutputOperator()
    {
      cleanTable();
    }

    public int getNumOfEventsInStore(String tableName)
    {
      Connection con;
      try {
        con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();

        String countQuery = "SELECT count(*) from " + tableName;
        ResultSet resultSet = stmt.executeQuery(countQuery);
        resultSet.next();
        return resultSet.getInt(1);
      } catch (SQLException e) {
        throw new RuntimeException("fetching count", e);
      }
    }

    public int getNumOfNullEventsInStore(String tableName)
    {
      Connection con;
      try {
        con = DriverManager.getConnection(URL);
        Statement stmt = con.createStatement();

        String countQuery = "SELECT count(*) from " + tableName + " where name1 is null";
        ResultSet resultSet = stmt.executeQuery(countQuery);
        resultSet.next();
        return resultSet.getInt(1);
      } catch (SQLException e) {
        throw new RuntimeException("fetching count", e);
      }
    }

    private static class TestPOJONonInsertOutputOperator extends JdbcPOJONonInsertOutputOperator
    {
      public TestPOJONonInsertOutputOperator()
      {
        cleanTable();
      }

      public int getNumOfEventsInStore()
      {
        Connection con;
        try {
          con = DriverManager.getConnection(URL);
          Statement stmt = con.createStatement();

          String countQuery = "SELECT count(*) from " + TABLE_POJO_NAME;
          ResultSet resultSet = stmt.executeQuery(countQuery);
          resultSet.next();
          return resultSet.getInt(1);
        } catch (SQLException e) {
          throw new RuntimeException("fetching count", e);
        }
      }

      public int getDistinctNonUnique()
      {
        Connection con;
        try {
          con = DriverManager.getConnection(URL);
          Statement stmt = con.createStatement();

          String countQuery = "SELECT count(distinct(name)) from " + TABLE_POJO_NAME;
          ResultSet resultSet = stmt.executeQuery(countQuery);
          resultSet.next();
          return resultSet.getInt(1);
        } catch (SQLException e) {
          throw new RuntimeException("fetching count", e);
        }
      }
    }
  }

  @Test
  public void testJdbcOutputOperator()
  {
    JdbcTransactionalStore transactionalStore = new JdbcTransactionalStore();
    transactionalStore.setDatabaseDriver(DB_DRIVER);
    transactionalStore.setDatabaseUrl(URL);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    TestOutputOperator outputOperator = new TestOutputOperator();
    outputOperator.setBatchSize(3);
    outputOperator.setStore(transactionalStore);

    outputOperator.setup(context);

    outputOperator.activate(context);
    List<TestEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new TestEvent(i));
    }

    outputOperator.beginWindow(0);
    for (TestEvent event : events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();

    Assert.assertEquals("rows in db", 10, outputOperator.getNumOfEventsInStore());
    cleanTable();
  }

  @Test
  public void testJdbcPojoOutputOperator()
  {
    JdbcTransactionalStore transactionalStore = new JdbcTransactionalStore();
    transactionalStore.setDatabaseDriver(DB_DRIVER);
    transactionalStore.setDatabaseUrl(URL);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    TestPOJOOutputOperator outputOperator = new TestPOJOOutputOperator();
    outputOperator.setBatchSize(3);
    outputOperator.setTablename(TABLE_POJO_NAME);

    List<JdbcFieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new JdbcFieldInfo("ID", "id", null, Types.INTEGER));
    fieldInfos.add(new JdbcFieldInfo("NAME", "name", null, Types.VARCHAR));
    outputOperator.setFieldInfos(fieldInfos);

    outputOperator.setStore(transactionalStore);

    outputOperator.setup(context);

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
    TestPortContext tpc = new TestPortContext(portAttributes);
    outputOperator.input.setup(tpc);

    outputOperator.activate(context);

    List<TestPOJOEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new TestPOJOEvent(i, "test" + i));
    }

    outputOperator.beginWindow(0);
    for (TestPOJOEvent event : events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();

    Assert.assertEquals("rows in db", 10, outputOperator.getNumOfEventsInStore(TABLE_POJO_NAME));
  }

  /**
   * This test will assume direct mapping for POJO fields to DB columns All
   * fields in DB present in POJO
   */
  @Test
  public void testJdbcPojoInsertOutputOperator()
  {
    JdbcTransactionalStore transactionalStore = new JdbcTransactionalStore();
    transactionalStore.setDatabaseDriver(DB_DRIVER);
    transactionalStore.setDatabaseUrl(URL);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    TestPOJOOutputOperator outputOperator = new TestPOJOOutputOperator();
    outputOperator.setBatchSize(3);
    outputOperator.setTablename(TABLE_POJO_NAME);

    outputOperator.setStore(transactionalStore);

    outputOperator.setup(context);

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
    TestPortContext tpc = new TestPortContext(portAttributes);
    outputOperator.input.setup(tpc);

    CollectorTestSink<Object> errorSink = new CollectorTestSink<>();
    TestUtils.setSink(outputOperator.error, errorSink);

    outputOperator.activate(context);

    List<TestPOJOEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new TestPOJOEvent(i, "test" + i));
    }
    events.add(new TestPOJOEvent(0, "test0")); // Records violating PK constraint
    events.add(new TestPOJOEvent(2, "test2")); // Records violating PK constraint
    events.add(new TestPOJOEvent(10, "test10")); // Clean record
    events.add(new TestPOJOEvent(11, "test11")); // Clean record
    events.add(new TestPOJOEvent(3, "test3")); // Records violating PK constraint
    events.add(new TestPOJOEvent(12, "test12")); // Clean record

    outputOperator.beginWindow(0);
    for (TestPOJOEvent event : events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();

    Assert.assertEquals("rows in db", 13, outputOperator.getNumOfEventsInStore(TABLE_POJO_NAME));
    Assert.assertEquals("Error tuples", 3, errorSink.collectedTuples.size());
  }

  /**
   * This test will assume direct mapping for POJO fields to DB columns All
   * fields in DB present in POJO and will test it for exactly once criteria
   */
  @Test
  public void testJdbcPojoInsertOutputOperatorExactlyOnce()
  {
    JdbcTransactionalStore transactionalStore = new JdbcTransactionalStore();
    transactionalStore.setDatabaseDriver(DB_DRIVER);
    transactionalStore.setDatabaseUrl(URL);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    TestPOJOOutputOperator outputOperator = new TestPOJOOutputOperator();
    outputOperator.setBatchSize(3);
    outputOperator.setTablename(TABLE_POJO_NAME);

    outputOperator.setStore(transactionalStore);

    outputOperator.setup(context);

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
    TestPortContext tpc = new TestPortContext(portAttributes);
    outputOperator.input.setup(tpc);

    CollectorTestSink<Object> errorSink = new CollectorTestSink<>();
    TestUtils.setSink(outputOperator.error, errorSink);

    outputOperator.activate(context);

    List<TestPOJOEvent> events = Lists.newArrayList();
    for (int i = 0; i < 70; i++) {
      events.add(new TestPOJOEvent(i, "test" + i));
    }

    outputOperator.beginWindow(0);
    for (int i = 0; i < 10; i++) {
      outputOperator.input.process(events.get(i));
    }
    outputOperator.endWindow();

    outputOperator.beginWindow(1);
    for (int i = 10; i < 20; i++) {
      outputOperator.input.process(events.get(i));
    }
    outputOperator.endWindow();

    outputOperator.beginWindow(2);
    for (int i = 20; i < 30; i++) {
      outputOperator.input.process(events.get(i));
    }
    outputOperator.endWindow();

    outputOperator.setup(context);
    outputOperator.input.setup(tpc);
    outputOperator.activate(context);

    outputOperator.beginWindow(0);
    for (int i = 30; i < 40; i++) {
      outputOperator.input.process(events.get(i));
    }
    outputOperator.endWindow();

    outputOperator.beginWindow(1);
    for (int i = 40; i < 50; i++) {
      outputOperator.input.process(events.get(i));
    }
    outputOperator.endWindow();

    outputOperator.beginWindow(2);
    for (int i = 50; i < 60; i++) {
      outputOperator.input.process(events.get(i));
    }

    outputOperator.beginWindow(3);
    for (int i = 60; i < 70; i++) {
      outputOperator.input.process(events.get(i));
    }
    outputOperator.endWindow();

    outputOperator.deactivate();
    outputOperator.teardown();

    Assert.assertEquals("rows in db", 40, outputOperator.getNumOfEventsInStore(TABLE_POJO_NAME));

  }


  /**
   * This test will assume direct mapping for POJO fields to DB columns Nullable
   * DB field missing in POJO name1 field, which is nullable in DB is missing
   * from POJO POJO(id, name) -> DB(id, name1)
   */
  @Test
  public void testJdbcPojoInsertOutputOperatorNullName()
  {
    JdbcTransactionalStore transactionalStore = new JdbcTransactionalStore();
    transactionalStore.setDatabaseDriver(DB_DRIVER);
    transactionalStore.setDatabaseUrl(URL);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    TestPOJOOutputOperator outputOperator = new TestPOJOOutputOperator();
    outputOperator.setBatchSize(3);
    outputOperator.setTablename(TABLE_POJO_NAME_NAME_DIFF);

    outputOperator.setStore(transactionalStore);

    outputOperator.setup(context);

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
    TestPortContext tpc = new TestPortContext(portAttributes);
    outputOperator.input.setup(tpc);

    outputOperator.activate(context);

    List<TestPOJOEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new TestPOJOEvent(i, "test" + i));
    }

    outputOperator.beginWindow(0);
    for (TestPOJOEvent event : events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();

    Assert.assertEquals("rows in db", 10, outputOperator.getNumOfEventsInStore(TABLE_POJO_NAME_NAME_DIFF));
    Assert
        .assertEquals("null name rows in db", 10, outputOperator.getNumOfNullEventsInStore(TABLE_POJO_NAME_NAME_DIFF));
  }

  /**
   * This test will assume direct mapping for POJO fields to DB columns.
   * Non-Nullable DB field missing in POJO id1 field which is non-nullable in DB
   * is missing from POJO POJO(id, name) -> DB(id1, name)
   */
  @Test
  public void testJdbcPojoInsertOutputOperatorNullId()
  {
    JdbcTransactionalStore transactionalStore = new JdbcTransactionalStore();
    transactionalStore.setDatabaseDriver(DB_DRIVER);
    transactionalStore.setDatabaseUrl(URL);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    TestPOJOOutputOperator outputOperator = new TestPOJOOutputOperator();
    outputOperator.setBatchSize(3);
    outputOperator.setTablename(TABLE_POJO_NAME_ID_DIFF);

    outputOperator.setStore(transactionalStore);

    outputOperator.setup(context);

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
    TestPortContext tpc = new TestPortContext(portAttributes);
    outputOperator.input.setup(tpc);

    boolean exceptionOccurred = false;
    try {
      outputOperator.activate(context);
    } catch (Exception e) {
      exceptionOccurred = true;
      Assert.assertTrue(e instanceof RuntimeException);
      Assert.assertTrue(e.getMessage().toLowerCase().contains("id1 not found in pojo"));
    }
    Assert.assertTrue(exceptionOccurred);
  }

  @Test
  public void testJdbcPojoOutputOperatorMerge()
  {
    JdbcTransactionalStore transactionalStore = new JdbcTransactionalStore();
    transactionalStore.setDatabaseDriver(DB_DRIVER);
    transactionalStore.setDatabaseUrl(URL);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    TestPOJOOutputOperator.TestPOJONonInsertOutputOperator updateOperator = new TestPOJOOutputOperator.TestPOJONonInsertOutputOperator();
    updateOperator.setBatchSize(3);

    updateOperator.setStore(transactionalStore);

    updateOperator.setSqlStatement("MERGE INTO " + TABLE_POJO_NAME + " AS T USING (VALUES (?, ?)) AS FOO(id, name) "
        + "ON T.id = FOO.id " + "WHEN MATCHED THEN UPDATE SET name = FOO.name "
        + "WHEN NOT MATCHED THEN INSERT( id, name ) VALUES (FOO.id, FOO.name);");

    List<JdbcFieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new JdbcFieldInfo("id", "id", null, Types.INTEGER));
    fieldInfos.add(new JdbcFieldInfo("name", "name", null, Types.VARCHAR));
    updateOperator.setFieldInfos(fieldInfos);
    updateOperator.setup(context);

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
    TestPortContext tpc = new TestPortContext(portAttributes);
    updateOperator.input.setup(tpc);

    updateOperator.activate(context);

    List<TestPOJOEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new TestPOJOEvent(i, "test" + i));
    }
    for (int i = 0; i < 5; i++) {
      events.add(new TestPOJOEvent(i, "test" + 100));
    }

    updateOperator.getDistinctNonUnique();
    updateOperator.beginWindow(0);
    for (TestPOJOEvent event : events) {
      updateOperator.input.process(event);
    }
    updateOperator.endWindow();

    // Expect 10 unique ids: 0 - 9
    Assert.assertEquals("rows in db", 10, updateOperator.getNumOfEventsInStore());
    // Expect 6 unique name: test-100, test-5, test-6, test-7, test-8, test-9
    Assert.assertEquals("rows in db", 6, updateOperator.getDistinctNonUnique());
  }

  @Test
  public void testJdbcInputOperator()
  {
    JdbcStore store = new JdbcStore();
    store.setDatabaseDriver(DB_DRIVER);
    store.setDatabaseUrl(URL);

    com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributeMap = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    TestInputOperator inputOperator = new TestInputOperator();
    inputOperator.setStore(store);
    insertEventsInTable(10);

    CollectorTestSink<Object> sink = new CollectorTestSink<>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(context);
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("rows from db", 10, sink.collectedTuples.size());
  }

  @Test
  public void testJdbcPojoInputOperator()
  {
    JdbcStore store = new JdbcStore();
    store.setDatabaseDriver(DB_DRIVER);
    store.setDatabaseUrl(URL);

    Attribute.AttributeMap.DefaultAttributeMap attributeMap = new Attribute.AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    insertEvents(10, true, 0);

    JdbcPOJOInputOperator inputOperator = new JdbcPOJOInputOperator();
    inputOperator.setStore(store);
    inputOperator.setTableName(TABLE_POJO_NAME);

    List<FieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new FieldInfo("ID", "id", null));
    fieldInfos.add(new FieldInfo("STARTDATE", "startDate", null));
    fieldInfos.add(new FieldInfo("STARTTIME", "startTime", null));
    fieldInfos.add(new FieldInfo("STARTTIMESTAMP", "startTimestamp", null));
    fieldInfos.add(new FieldInfo("SCORE", "score", FieldInfo.SupportType.DOUBLE));
    inputOperator.setFieldInfos(fieldInfos);

    inputOperator.setFetchSize(5);

    CollectorTestSink<Object> sink = new CollectorTestSink<>();
    inputOperator.outputPort.setSink(sink);

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPOJOEvent.class);
    TestPortContext tpc = new TestPortContext(portAttributes);

    inputOperator.setup(context);
    inputOperator.outputPort.setup(tpc);

    inputOperator.activate(context);

    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("rows from db", 5, sink.collectedTuples.size());
    int i = 0;
    for (Object tuple : sink.collectedTuples) {
      TestPOJOEvent pojoEvent = (TestPOJOEvent)tuple;
      Assert.assertTrue("i=" + i, pojoEvent.getId() == i);
      Assert.assertTrue("date", pojoEvent.getStartDate() instanceof Date);
      Assert.assertTrue("time", pojoEvent.getStartTime() instanceof Time);
      Assert.assertTrue("timestamp", pojoEvent.getStartTimestamp() instanceof Timestamp);
      i++;
    }
    sink.collectedTuples.clear();

    inputOperator.beginWindow(1);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("rows from db", 5, sink.collectedTuples.size());
    for (Object tuple : sink.collectedTuples) {
      TestPOJOEvent pojoEvent = (TestPOJOEvent)tuple;
      Assert.assertTrue("i=" + i, pojoEvent.getId() == i);
      Assert.assertTrue("date", pojoEvent.getStartDate() instanceof Date);
      Assert.assertTrue("time", pojoEvent.getStartTime() instanceof Time);
      Assert.assertTrue("timestamp", pojoEvent.getStartTimestamp() instanceof Timestamp);
      Assert.assertTrue("score", pojoEvent.getScore() == 55.4);
      i++;
    }

    sink.collectedTuples.clear();

    inputOperator.beginWindow(2);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("rows from db", 0, sink.collectedTuples.size());

    // Insert 3 more tuples and check if they are read successfully.
    insertEvents(3, false, 10);

    inputOperator.beginWindow(3);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("rows from db", 3, sink.collectedTuples.size());
    for (Object tuple : sink.collectedTuples) {
      TestPOJOEvent pojoEvent = (TestPOJOEvent)tuple;
      Assert.assertTrue("i=" + i, pojoEvent.getId() == i);
      Assert.assertTrue("date", pojoEvent.getStartDate() instanceof Date);
      Assert.assertTrue("time", pojoEvent.getStartTime() instanceof Time);
      Assert.assertTrue("timestamp", pojoEvent.getStartTimestamp() instanceof Timestamp);
      Assert.assertTrue("score", pojoEvent.getScore() == 55.4);
      i++;
    }
  }

}
