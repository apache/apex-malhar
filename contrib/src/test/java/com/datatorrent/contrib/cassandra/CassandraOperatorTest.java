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
import java.util.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.AfterClass;

/**
 * Tests for {@link AbstractCassandraTransactionableOutputOperator} and {@link AbstractCassandraInputOperator}
 */
public class CassandraOperatorTest
{
  public static final String NODE = "localhost";
  public static final String KEYSPACE = "demo";

  private static final String TABLE_NAME = "test";
  private static String APP_ID = "CassandraOperatorTest";
  private static int OPERATOR_ID = 0;
  private static Cluster cluster = null;
  private static Session session=null;

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
       cluster = Cluster.builder()
          .addContactPoint(NODE).build();
      session = cluster.connect(KEYSPACE);

      String createMetaTable = "CREATE TABLE IF NOT EXISTS " + CassandraTransactionalStore.DEFAULT_META_TABLE + " ( "
              + CassandraTransactionalStore.DEFAULT_APP_ID_COL + " TEXT, "
              + CassandraTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT, "
              + CassandraTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT, "
              + "PRIMARY KEY (" + CassandraTransactionalStore.DEFAULT_APP_ID_COL + ", " + CassandraTransactionalStore.DEFAULT_OPERATOR_ID_COL + ") "
              + ");";
      session.execute(createMetaTable);
      String createTable = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE_NAME + " (id uuid PRIMARY KEY,age int,lastname text,test boolean,floatvalue float,doubleValue double,set1 set<int>,list1 list<int>,map1 map<text,int>);";
      session.execute(createTable);
    }
    catch (Throwable e) {
      DTThrowable.rethrow(e);
    }
  }

  @AfterClass
  public static void cleanup()
  {
   if(session!=null)
   {
     session.execute("DROP TABLE " + CassandraTransactionalStore.DEFAULT_META_TABLE);
     session.execute("DROP TABLE " +  KEYSPACE + "." + TABLE_NAME);
     session.close();
   }
    if(cluster!=null)
    {
      cluster.close();
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

    TestOutputOperator()
    {
      //cleanTable();
    }

    public long getNumOfEventsInStore()
    {

      try {
        Cluster cluster = Cluster.builder()
            .addContactPoint(NODE).build();
        Session session = cluster.connect(KEYSPACE);

        String countQuery = "SELECT count(*) from " + TABLE_NAME + ";";
        ResultSet resultSetCount = session.execute(countQuery);
        for(Row row: resultSetCount)
        {
          return row.getLong(0);
        }
        return 0;
      }
      catch (DriverException e) {
        throw new RuntimeException("fetching count", e);
      }
    }

    public String getEventsInStore()
    {
      try {
        Cluster cluster = Cluster.builder()
            .addContactPoint(NODE).build();
        Session session = cluster.connect(KEYSPACE);

        String recordsQuery = "SELECT * from " + TABLE_NAME + ";";
        ResultSet resultSetRecords = session.execute(recordsQuery);
         for(Row row: resultSetRecords)
        {
          System.out.println("result is "+ row.getBool("test") + "," + row.getString("lastname"));
        }
        return null;
      }
      catch (DriverException e) {
        throw new RuntimeException("fetching records", e);
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

    outputOperator.setTablename(TABLE_NAME);
    ArrayList<String> columns = new ArrayList<String>();
    columns.add("id");
    columns.add("age");
   // columns.add("lastname");
  //  columns.add("test");
  //  columns.add("floatValue");
    columns.add("doubleValue");
    columns.add("floatValue");
     columns.add("lastname");
   // columns.add("date");
   // columns.add("set1");
    columns.add("list1");
    columns.add("map1");
    columns.add("set1");
    columns.add("test");
    // columns.add("age");
    outputOperator.setColumns(columns);
    ArrayList<String> expressions = new ArrayList<String>();
    expressions.add("id");
    expressions.add("age");
 //   expressions.add("lastname");
 //   expressions.add("test");
  //  expressions.add("floatValue");
    expressions.add("doubleValue");
    expressions.add("floatValue");
    expressions.add("lastname");
    //expressions.add("date");
    //expressions.add("set1");
    expressions.add("list1");
    expressions.add("map1");
    expressions.add("set1");
   expressions.add("test");
    // expressions.add("age");
    outputOperator.setExpressions(expressions);
    outputOperator.setStore(transactionalStore);

    outputOperator.setup(context);

    List<TestPojo> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      Set<Integer> set  = new HashSet<Integer>();
      set.add(i);
      List<Integer> list = new ArrayList<Integer>();
      list.add(i);
      Map<String,Integer> map = new HashMap<String, Integer>();
      map.put("key"+i, i);
      events.add(new TestPojo(UUID.randomUUID(), i, "abclast" + i,true,i,2.0,set,list,map));
    }

    outputOperator.beginWindow(0);
    for (TestPojo event: events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();

    Assert.assertEquals("rows in db", 10, outputOperator.getNumOfEventsInStore());
    outputOperator.getEventsInStore();
    for (int i = 0; i < 10; i++) {

      //StringBuilder firstlastname = new StringBuilder("abc"+i+","+"abclast"+i);
      //Assert.assertEquals(firstlastname.toString(),outputOperator.getEventsInStore());
     }

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

  public static class TestPojo
  {
    private int age = 2;

    private TestPojo(UUID randomUUID, int i, String string, boolean b, float d, double d0, Set<Integer> set1, List<Integer> list1, Map<String, Integer> map1)
    {
      this.id = randomUUID;
      this.age = i;
      this.lastname = string;
      this.test = b;
      this.floatValue = d;
      this.doubleValue = d0;
     // this.date = date;
      this.set1 = set1;
      this.list1 = list1;
      this.map1 = map1;
    }

    public int getAge()
    {
      return age;
    }

    public void setAge(int age)
    {
      this.age = age;
    }

    public boolean isTest()
    {
      return test;
    }

    public void setTest(boolean test)
    {
      this.test = test;
    }

    public Set getSet1()
    {
      return set1;
    }

    public void setSet1(Set set)
    {
      this.set1 = set;
    }

    public List getList1()
    {
      return list1;
    }

    public void setList1(List list)
    {
      this.list1 = list;
    }

    public Map<String, Integer> getMap1()
    {
      return map1;
    }

    public void setMap1(Map<String, Integer> map)
    {
      this.map1 = map;
    }

    public Date getDate()
    {
      return date;
    }

    public void setDate(Date date)
    {
      this.date = date;
    }

    public Double getDoubleValue()
    {
      return doubleValue;
    }

    public void setDoubleValue(Double doubleValue)
    {
      this.doubleValue = doubleValue;
    }

    public Float getFloatValue()
    {
      return floatValue;
    }

    public void setFloatValue(Float floatValue)
    {
      this.floatValue = floatValue;
    }
    private String lastname = "hello";
    private UUID id;
    private boolean test;
    private Set set1;
    private List list1;
    private Map<String,Integer> map1;
    private Date date;
    private Double doubleValue;
    private Float  floatValue;

    public UUID getId()
    {
      return id;
    }

    public void setId(UUID id)
    {
      this.id = id;
    }

    public String getLastname()
    {
      return lastname;
    }

    public void setLastname(String lastname)
    {
      this.lastname = lastname;
    }



  }

}


