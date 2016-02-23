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
package com.datatorrent.contrib.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

import com.datatorrent.lib.helper.TestPortContext;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.netlet.util.DTThrowable;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.google.common.collect.Lists;
import java.util.*;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for {@link AbstractCassandraTransactionableOutputOperator} and {@link AbstractCassandraInputOperator}
 */
public class CassandraOperatorTest
{
  public static final String NODE = "localhost";
  public static final String KEYSPACE = "demo";

  private static final String TABLE_NAME = "test";
  private static final String TABLE_NAME_INPUT = "testinput";
  private static final String APP_ID = "CassandraOperatorTest";
  private static final int OPERATOR_ID = 0;
  private static Cluster cluster = null;
  private static Session session = null;

  @SuppressWarnings("unused")
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
    @SuppressWarnings("UnusedDeclaration") Class<?> clazz = org.codehaus.janino.CompilerFactory.class;
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
      String createTable = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE_NAME + " (id uuid PRIMARY KEY,age int,lastname text,test boolean,floatvalue float,doubleValue double,set1 set<int>,list1 list<int>,map1 map<text,int>,last_visited timestamp);";
      session.execute(createTable);
      createTable = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE_NAME_INPUT + " (id int PRIMARY KEY,lastname text,age int);";
      session.execute(createTable);
    }
    catch (Throwable e) {
      DTThrowable.rethrow(e);
    }
  }

  @AfterClass
  public static void cleanup()
  {
    if (session != null) {
      session.execute("DROP TABLE " + CassandraTransactionalStore.DEFAULT_META_TABLE);
      session.execute("DROP TABLE " + KEYSPACE + "." + TABLE_NAME);
      session.execute("DROP TABLE " + KEYSPACE + "." + TABLE_NAME_INPUT);
      session.close();
    }
    if (cluster != null) {
      cluster.close();
    }
  }

  private static class TestOutputOperator extends CassandraPOJOOutputOperator
  {
    public long getNumOfEventsInStore()
    {
      String countQuery = "SELECT count(*) from " + TABLE_NAME + ";";
      ResultSet resultSetCount = session.execute(countQuery);
      for (Row row: resultSetCount) {
        return row.getLong(0);
      }
      return 0;

    }

    public void getEventsInStore()
    {
      String recordsQuery = "SELECT * from " + TABLE_NAME + ";";
      ResultSet resultSetRecords = session.execute(recordsQuery);
      for (Row row: resultSetRecords) {
        int age = row.getInt("age");
        Assert.assertEquals("check boolean", true, row.getBool("test"));
        Assert.assertEquals("check last name", "abclast", row.getString("lastname"));
        Assert.assertEquals("check double", 2.0, row.getDouble("doubleValue"), 2);
        LOG.debug("age returned is {}", age);
        Assert.assertNotEquals("check date", new Date(System.currentTimeMillis()), row.getDate("last_visited"));
        if (age == 2) {
          Assert.assertEquals("check float", 2.0, row.getFloat("floatValue"), 2);
          Set<Integer> set = new HashSet<Integer>();
          List<Integer> list = new ArrayList<Integer>();
          Map<String, Integer> map = new HashMap<String, Integer>();
          set.add(2);
          list.add(2);
          map.put("key2", 2);

          Assert.assertEquals("check set", set, row.getSet("set1", Integer.class));
          Assert.assertEquals("check map", map, row.getMap("map1", String.class, Integer.class));
          Assert.assertEquals("check list", list, row.getList("list1", Integer.class));
        }
        if (age == 0) {
          Assert.assertEquals("check float", 0.0, row.getFloat("floatValue"), 2);
          Set<Integer> set = new HashSet<Integer>();
          List<Integer> list = new ArrayList<Integer>();
          Map<String, Integer> map = new HashMap<String, Integer>();
          set.add(0);
          list.add(0);
          map.put("key0", 0);
          Assert.assertEquals("check set", set, row.getSet("set1", Integer.class));
          Assert.assertEquals("check map", map, row.getMap("map1", String.class, Integer.class));
          Assert.assertEquals("check list", list, row.getList("list1", Integer.class));
        }
        if (age == 1) {
          Assert.assertEquals("check float", 1.0, row.getFloat("floatValue"), 2);
          Set<Integer> set = new HashSet<Integer>();
          List<Integer> list = new ArrayList<Integer>();
          Map<String, Integer> map = new HashMap<String, Integer>();
          set.add(1);
          list.add(1);
          map.put("key1", 1);
          Assert.assertEquals("check set", set, row.getSet("set1", Integer.class));
          Assert.assertEquals("check map", map, row.getMap("map1", String.class, Integer.class));
          Assert.assertEquals("check list", list, row.getList("list1", Integer.class));
        }
      }
    }
  }

  private static class TestInputOperator extends CassandraPOJOInputOperator
  {
    private final ArrayList<Integer> ids = new ArrayList<Integer>();
    private final HashMap<Integer, String> mapNames = new HashMap<Integer, String>();
    private final HashMap<Integer, Integer> mapAge = new HashMap<Integer, Integer>();

    public void insertEventsInTable(int numEvents)
    {
      try {
        Cluster cluster = Cluster.builder()
                .addContactPoint(NODE).build();
        Session session = cluster.connect(KEYSPACE);

        String insert = "INSERT INTO " + TABLE_NAME_INPUT + " (ID,lastname,age)" + " VALUES (?,?,?);";
        PreparedStatement stmt = session.prepare(insert);
        BoundStatement boundStatement = new BoundStatement(stmt);
        for (int i = 0; i < numEvents; i++) {
          ids.add(i);
          mapNames.put(i, "test" + i);
          mapAge.put(i, i + 10);
          session.execute(boundStatement.bind(i, "test" + i, i + 10));
        }
      }
      catch (DriverException e) {
        throw new RuntimeException(e);
      }
    }

    public ArrayList<Integer> getIds()
    {
      return ids;
    }

    public HashMap<Integer, String> getNames()
    {
      return mapNames;
    }

    public HashMap<Integer, Integer> getAge()
    {
      return mapAge;
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
    List<FieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new FieldInfo("id", "id", null));
    fieldInfos.add(new FieldInfo("age", "age", null));
    fieldInfos.add(new FieldInfo("doubleValue", "doubleValue", null));
    fieldInfos.add(new FieldInfo("floatValue", "floatValue", null));
    fieldInfos.add(new FieldInfo("last_visited", "last_visited", null));
    fieldInfos.add(new FieldInfo("lastname", "lastname", null));
    fieldInfos.add(new FieldInfo("list1", "list1", null));
    fieldInfos.add(new FieldInfo("map1", "map1", null));
    fieldInfos.add(new FieldInfo("set1", "set1", null));
    fieldInfos.add(new FieldInfo("test", "test", null));

    outputOperator.setStore(transactionalStore);
    outputOperator.setFieldInfos(fieldInfos);
    outputOperator.setup(context);

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestPojo.class);
    TestPortContext tpc = new TestPortContext(portAttributes);

    outputOperator.input.setup(tpc);
    outputOperator.activate(context);

    List<TestPojo> events = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      Set<Integer> set = new HashSet<Integer>();
      set.add(i);
      List<Integer> list = new ArrayList<Integer>();
      list.add(i);
      Map<String, Integer> map = new HashMap<String, Integer>();
      map.put("key" + i, i);
      events.add(new TestPojo(UUID.randomUUID(), i, "abclast", true, i, 2.0, set, list, map, new Date(System.currentTimeMillis())));
    }

    outputOperator.beginWindow(0);
    for (TestPojo event : events) {
      outputOperator.input.process(event);
    }
    outputOperator.endWindow();

    Assert.assertEquals("rows in db", 3, outputOperator.getNumOfEventsInStore());
    outputOperator.getEventsInStore();
  }

  /*
   * This test can be run on cassandra server installed on node17.
   */
  @Test
  public void testCassandraInputOperator()
  {
    String query1 = "SELECT * FROM " + KEYSPACE + "." + "%t;";
    CassandraStore store = new CassandraStore();
    store.setNode(NODE);
    store.setKeyspace(KEYSPACE);

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    TestInputOperator inputOperator = new TestInputOperator();
    inputOperator.setStore(store);
    inputOperator.setQuery(query1);
    inputOperator.setTablename(TABLE_NAME_INPUT);
    inputOperator.setPrimaryKeyColumn("id");

    List<FieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new FieldInfo("id", "id", null));
    fieldInfos.add(new FieldInfo("age", "age", null));
    fieldInfos.add(new FieldInfo("lastname", "lastname", null));
    inputOperator.setFieldInfos(fieldInfos);

    inputOperator.insertEventsInTable(30);
    CollectorTestSink<Object> sink = new CollectorTestSink<>();
    inputOperator.outputPort.setSink(sink);

    Attribute.AttributeMap.DefaultAttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
    portAttributes.put(Context.PortContext.TUPLE_CLASS, TestInputPojo.class);
    TestPortContext tpc = new TestPortContext(portAttributes);

    inputOperator.setup(context);
    inputOperator.outputPort.setup(tpc);
    inputOperator.activate(context);

    inputOperator.beginWindow(0);
    inputOperator.emitTuples();
    inputOperator.endWindow();
    Assert.assertEquals("rows from db", 30, sink.collectedTuples.size());
    ArrayList<Integer> listOfIDs = inputOperator.getIds();
    // Rows are not stored in the same order in cassandra table in which they are inserted.
    for (int i = 0; i < 10; i++) {
      TestInputPojo object = (TestInputPojo)sink.collectedTuples.get(i);
      Assert.assertTrue("id set in testpojo", listOfIDs.contains(object.getId()));
      Assert.assertEquals("name set in testpojo", inputOperator.getNames().get(object.getId()), object.getLastname());
      Assert.assertEquals("age set in testpojo", inputOperator.getAge().get(object.getId()).intValue(), object.getAge());
    }

    sink.clear();
    inputOperator.columnDataTypes.clear();

    String query2 = "SELECT * FROM " + KEYSPACE + "." + "%t where token(%p) > %v;";
    inputOperator.setQuery(query2);
    inputOperator.setup(context);
    inputOperator.outputPort.setup(tpc);
    inputOperator.activate(context);

    inputOperator.setStartRow(10);
    inputOperator.beginWindow(1);
    inputOperator.emitTuples();
    inputOperator.endWindow();
    Assert.assertEquals("rows from db", 14, sink.collectedTuples.size());

    sink.clear();
    inputOperator.columnDataTypes.clear();

    String query3 = "SELECT * FROM " + KEYSPACE + "." + "%t where token(%p) > %v LIMIT %l;";
    inputOperator.setQuery(query3);
    inputOperator.setup(context);
    inputOperator.outputPort.setup(tpc);
    inputOperator.activate(context);

    inputOperator.setStartRow(1);
    inputOperator.setLimit(10);
    inputOperator.beginWindow(2);
    inputOperator.emitTuples();
    inputOperator.endWindow();
    Assert.assertEquals("rows from db", 10, sink.collectedTuples.size());
  }

  public static class TestPojo
  {
    public TestPojo(UUID randomUUID, int i, String string, boolean b, float d, double d0, Set<Integer> set1, List<Integer> list1, Map<String, Integer> map1, Date date)
    {
      this.id = randomUUID;
      this.age = i;
      this.lastname = string;
      this.test = b;
      this.floatValue = d;
      this.doubleValue = d0;
      this.set1 = set1;
      this.list1 = list1;
      this.map1 = map1;
      this.last_visited = date;
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

    public Set<Integer> getSet1()
    {
      return set1;
    }

    public void setSet1(Set<Integer> set)
    {
      this.set1 = set;
    }

    public List<Integer> getList1()
    {
      return list1;
    }

    public void setList1(List<Integer> list)
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
    private Set<Integer> set1;
    private List<Integer> list1;
    private Map<String, Integer> map1;
    private Double doubleValue;
    private Float floatValue;
    private Date last_visited;
    private int age = 2;

    public Date getLast_visited()
    {
      return last_visited;
    }

    public void setLast_visited(Date last_visited)
    {
      this.last_visited = last_visited;
    }

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

  private static final Logger LOG = LoggerFactory.getLogger(CassandraOperatorTest.class);

}
