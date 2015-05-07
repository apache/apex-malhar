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
package com.datatorrent.contrib.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Tests for {@link AbstractCassandraTransactionableOutputOperator} and {@link AbstractCassandraInputOperator}
 */
public class CassandraOperatorTest
{
  public static final String NODE = "localhost";
  public static final String KEYSPACE = "demo";

  private static final String TABLE_NAME = "test_event_table";
  private static String APP_ID = "CassandraOperatorTest";
  private static int OPERATOR_ID = 0;

  private static class TestEvent
  {
    int id;

    TestEvent(int id)
    {
      this.id = id;
    }
  }

  @BeforeClass
  public static void setup()
  {
    try {
      Cluster cluster = Cluster.builder()
          .addContactPoint(NODE).build();
      Session session = cluster.connect(KEYSPACE);


      String createMetaTable = "CREATE TABLE IF NOT EXISTS " + CassandraTransactionalStore.DEFAULT_META_TABLE + " ( " +
          CassandraTransactionalStore.DEFAULT_APP_ID_COL + " TEXT, " +
          CassandraTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT, " +
          CassandraTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT, " +
          "PRIMARY KEY (" + CassandraTransactionalStore.DEFAULT_APP_ID_COL + ", " + CassandraTransactionalStore.DEFAULT_OPERATOR_ID_COL  + ") " +
          ");";
      session.execute(createMetaTable);
      String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (ID INT PRIMARY KEY);";
      session.execute(createTable);
    }
    catch (Throwable e) {

      DTThrowable.rethrow(e);
    }
  }

  private static void cleanTable()
  {
    try {
      Cluster cluster = Cluster.builder()
          .addContactPoint(NODE).build();
      Session session = cluster.connect(KEYSPACE);

      String cleanTable = "TRUNCATE " + TABLE_NAME + ";";
      session.execute(cleanTable);
    }
    catch (DriverException e) {
      throw new RuntimeException(e);
    }
  }

  private static class TestOutputOperator extends CassandraOutputOperator
  {
    //private static final String INSERT_STMT = "INSERT INTO " + KEYSPACE+"." +TABLE_NAME + " (ID) VALUES (?);";

    /*TestOutputOperator()
    {
      cleanTable();
    }

    @Nonnull
    @Override
    protected PreparedStatement getUpdateCommand()
    {
      try {
        return store.getSession().prepare(INSERT_STMT);
      }
      catch (DriverException e) {
        throw new RuntimeException("preparing", e);
      }
    }

    @Override
    protected Statement setStatementParameters(PreparedStatement statement, TestEvent tuple) throws DriverException
    {
      BoundStatement boundStatement = new BoundStatement(statement);
      Statement stmnt = boundStatement.bind(tuple.id);
      return stmnt;
    }*/

    public long getNumOfEventsInStore()
    {

      try {
        Cluster cluster = Cluster.builder()
            .addContactPoint(NODE).build();
        Session session = cluster.connect(KEYSPACE);

        String countQuery = "SELECT count(*) from " + TABLE_NAME + ";";
        ResultSet resultSet = session.execute(countQuery);
        for(Row row: resultSet)
        {
          return row.getLong(0);
        }
        return 0;
      }
      catch (DriverException e) {
        throw new RuntimeException("fetching count", e);
      }
    }
  }

  private static class TestInputOperator extends AbstractCassandraInputOperator<TestEvent>
  {

    private static final String retrieveQuery = "SELECT * FROM " +KEYSPACE +"."+TABLE_NAME + ";";

    TestInputOperator()
    {
      cleanTable();
    }

    @Override
    public TestEvent getTuple(Row row)
    {
      try {
        return new TestEvent(row.getInt(0));
      }
      catch (DriverException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String queryToRetrieveData()
    {
      return retrieveQuery;
    }

    public void insertEventsInTable(int numEvents)
    {
      try {
        Cluster cluster = Cluster.builder()
            .addContactPoint(NODE).build();
        Session session = cluster.connect(KEYSPACE);

        String insert = "INSERT INTO " + TABLE_NAME +" (ID)"+ " VALUES (?);";
        PreparedStatement stmt = session.prepare(insert);
        BoundStatement boundStatement = new BoundStatement(stmt);
        Statement statement;
        for (int i = 0; i < numEvents; i++) {
          statement = boundStatement.bind(i);
          session.execute(statement);
        }
      }
      catch (DriverException e) {
        throw new RuntimeException(e);
      }
    }
  }


  @Test
  public void testCassandraOutputOperator()
  {
    CassandraTransactionalStore transactionalStore = new CassandraTransactionalStore();
    transactionalStore.setNode(NODE);
    transactionalStore.setKeyspace(KEYSPACE);

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    TestOutputOperator outputOperator = new TestOutputOperator();
   // outputOperator.setKeyspace(KEYSPACE);
    outputOperator.setTablename(TABLE_NAME);
    ArrayList<String> columns = new ArrayList<String>();
    columns.add("ID");
    outputOperator.setColumns(columns);
    ArrayList<String> expressions = new ArrayList<String>();
    expressions.add("getID()");
    outputOperator.setExpressions(expressions);
    outputOperator.setStore(transactionalStore);

    outputOperator.setup(context);

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
  }

  //@Test
  public void TestCassandraInputOperator()
  {
    CassandraStore store = new CassandraStore();
    store.setNode(NODE);
    store.setKeyspace(KEYSPACE);

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    TestInputOperator inputOperator = new TestInputOperator();
    inputOperator.setStore(store);
    inputOperator.insertEventsInTable(10);

    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    inputOperator.outputPort.setSink(sink);

    inputOperator.setup(context);
    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();

    Assert.assertEquals("rows from db", 10, sink.collectedTuples.size());
  }

   public InnerObj innerObj = new InnerObj();

  /**
   * @return the innerObj
   */
  public InnerObj getInnerObj()
  {
    return innerObj;
  }

  /**
   * @param innerObj the innerObj to set
   */
  public void setInnerObj(InnerObj innerObj)
  {
    this.innerObj = innerObj;
  }

  public class InnerObj
  {
    public InnerObj()
    {
    }

    private int ID=11;

    /**
     * @return the int ID
     */
    public int getID()
    {
      return ID;
    }

    /**
     * @param ID the intVal to set
     */
    public void setID(int ID)
    {
      this.ID = ID;
    }

  }
}

