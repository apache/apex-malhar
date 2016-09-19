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
package org.apache.apex.malhar.lib.join;

import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import com.datatorrent.stram.engine.PortContext;

public class POJOInnerJoinOperatorTest
{
  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();
  private static final String APPLICATION_PATH_PREFIX = "target/InnerJoinPOJOTest/";
  private String applicationPath;
  private Attribute.AttributeMap.DefaultAttributeMap attributes;
  Context.OperatorContext context;

  public static class Customer
  {
    public int ID;
    public String Name;
    public Customer()
    {
    }

    public Customer(int ID, String name)
    {
      this.ID = ID;
      Name = name;
    }

    @Override
    public String toString()
    {
      return "Customer{" +
        "ID=" + ID +
        ", Name='" + Name + '\'' +
        '}';
    }
  }

  public static class Order
  {
    public int OID;
    public int CID;
    public int Amount;

    public Order()
    {
    }

    public Order(int OID, int CID, int amount)
    {
      this.OID = OID;
      this.CID = CID;
      Amount = amount;
    }

    @Override
    public String toString()
    {
      return "Order{" +
        "OID=" + OID +
        ", CID=" + CID +
        ", Amount=" + Amount +
        '}';
    }
  }

  public static class CustOrder
  {
    public int ID;
    public String Name;
    public int OID;
    public int Amount;

    public CustOrder()
    {
    }

    @Override
    public String toString()
    {
      return "{" +
        "ID=" + ID +
        ", Name='" + Name + '\'' +
        ", OID=" + OID +
        ", Amount=" + Amount +
        '}';
    }
  }

  @Before
  public void beforeTest()
  {
    applicationPath =  OperatorContextTestHelper.getUniqueApplicationPath(APPLICATION_PATH_PREFIX);
    attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    context = new OperatorContextTestHelper.TestIdOperatorContext(1, attributes);
  }

  @After
  public void afterTest()
  {
    Path root = new Path(applicationPath);
    try {
      FileSystem fs = FileSystem.newInstance(root.toUri(), new Configuration());
      fs.delete(root, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testInnerJoinOperator() throws IOException, InterruptedException
  {
    POJOInnerJoinOperator oper = new POJOInnerJoinOperator();
    oper.setIncludeFieldStr("ID,Name;OID,Amount");
    oper.setLeftKeyExpression("ID");
    oper.setRightKeyExpression("CID");
    oper.setExpiryTime(10000L);

    oper.setup(context);
    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, CustOrder.class);
    oper.outputPort.setup(new PortContext(attributes,context));

    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, Customer.class);
    oper.input1.setup(new PortContext(attributes,context));
    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, Order.class);
    oper.input2.setup(new PortContext(attributes,context));
    oper.activate(context);

    CollectorTestSink<CustOrder> sink = new CollectorTestSink<>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink)sink;
    oper.outputPort.setSink(tmp);
    oper.beginWindow(0);

    Customer tuple = new Customer(1, "Anil");

    oper.input1.process(tuple);
    Order order = new Order(102, 1, 300);

    oper.input2.process(order);
    Order order2 = new Order(103, 3, 300);
    oper.input2.process(order2);
    Order order3 = new Order(104, 7, 300);
    oper.input2.process(order3);

    oper.endWindow();

    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 1, sink.collectedTuples.size());
    CustOrder emitted = sink.collectedTuples.iterator().next();

    Assert.assertEquals("value of ID :", tuple.ID, emitted.ID);
    Assert.assertEquals("value of Name :", tuple.Name, emitted.Name);

    Assert.assertEquals("value of OID: ", order.OID, emitted.OID);
    Assert.assertEquals("value of Amount: ", order.Amount, emitted.Amount);

    oper.teardown();
  }

  @Test
  public void testMultipleValues() throws IOException, InterruptedException
  {
    POJOInnerJoinOperator oper = new POJOInnerJoinOperator();
    oper.setIncludeFieldStr("ID,Name;OID,Amount");
    oper.setLeftKeyExpression("ID");
    oper.setRightKeyExpression("CID");
    oper.setExpiryTime(10000L);

    oper.setup(context);
    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, CustOrder.class);
    oper.outputPort.setup(new PortContext(attributes,context));

    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, Customer.class);
    oper.input1.setup(new PortContext(attributes,context));
    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, Order.class);
    oper.input2.setup(new PortContext(attributes,context));
    oper.activate(context);

    CollectorTestSink<CustOrder> sink = new CollectorTestSink<>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink)sink;
    oper.outputPort.setSink(tmp);

    oper.beginWindow(0);
    Order order = new Order(102, 1, 300);
    oper.input2.process(order);

    Order order2 = new Order(103, 3, 300);
    oper.input2.process(order2);
    oper.endWindow();
    oper.beginWindow(1);

    Order order3 = new Order(104, 1, 300);
    oper.input2.process(order3);
    Customer tuple = new Customer(1, "Anil");
    oper.input1.process(tuple);
    oper.endWindow();

    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 2, sink.collectedTuples.size());
    CustOrder emitted = sink.collectedTuples.get(0);

    Assert.assertEquals("value of ID :", tuple.ID, emitted.ID);
    Assert.assertEquals("value of Name :", tuple.Name, emitted.Name);
    Assert.assertEquals("value of OID: ", order.OID, emitted.OID);
    Assert.assertEquals("value of Amount: ", order.Amount, emitted.Amount);

    emitted = sink.collectedTuples.get(1);
    Assert.assertEquals("value of ID :", tuple.ID, emitted.ID);
    Assert.assertEquals("value of Name :", tuple.Name, emitted.Name);
    Assert.assertEquals("value of OID: ", order3.OID, emitted.OID);
    Assert.assertEquals("value of Amount: ", order3.Amount, emitted.Amount);
    oper.teardown();
  }

  @Test
  public void testUpdateStream1Values() throws IOException, InterruptedException
  {
    POJOInnerJoinOperator oper = new POJOInnerJoinOperator();
    oper.setIncludeFieldStr("ID,Name;OID,Amount");
    oper.setLeftKeyExpression("ID");
    oper.setRightKeyExpression("CID");
    oper.setLeftKeyPrimary(true);
    oper.setExpiryTime(10000L);

    oper.setup(context);
    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, CustOrder.class);
    oper.outputPort.setup(new PortContext(attributes,context));

    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, Customer.class);
    oper.input1.setup(new PortContext(attributes,context));
    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, Order.class);
    oper.input2.setup(new PortContext(attributes,context));
    oper.activate(context);

    CollectorTestSink<CustOrder> sink = new CollectorTestSink<>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink)sink;
    oper.outputPort.setSink(tmp);

    oper.beginWindow(0);
    Customer tuple1 = new Customer(1, "Anil");
    oper.input1.process(tuple1);
    oper.endWindow();
    oper.beginWindow(1);

    Customer tuple2 = new Customer(1, "Join");
    oper.input1.process(tuple2);
    Order order = new Order(102, 1, 300);
    oper.input2.process(order);
    Order order2 = new Order(103, 3, 300);
    oper.input2.process(order2);
    oper.endWindow();

    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 1, sink.collectedTuples.size());
    CustOrder emitted = sink.collectedTuples.get(0);

    Assert.assertEquals("value of ID :", tuple2.ID, emitted.ID);
    Assert.assertEquals("value of Name :", tuple2.Name, emitted.Name);
    Assert.assertEquals("value of OID: ", order.OID, emitted.OID);
    Assert.assertEquals("value of Amount: ", order.Amount, emitted.Amount);
    oper.teardown();
  }

  @Test
  public void testEmitMultipleTuplesFromStream2() throws IOException, InterruptedException
  {
    POJOInnerJoinOperator oper = new POJOInnerJoinOperator();
    oper.setIncludeFieldStr("ID,Name;OID,Amount");
    oper.setLeftKeyExpression("ID");
    oper.setRightKeyExpression("CID");
    oper.setLeftKeyPrimary(true);
    oper.setExpiryTime(10000L);

    oper.setup(context);
    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, CustOrder.class);
    oper.outputPort.setup(new PortContext(attributes,context));

    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, Customer.class);
    oper.input1.setup(new PortContext(attributes,context));
    attributes.put(DAG.InputPortMeta.TUPLE_CLASS, Order.class);
    oper.input2.setup(new PortContext(attributes,context));
    oper.activate(context);

    CollectorTestSink<CustOrder> sink = new CollectorTestSink<>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink)sink;
    oper.outputPort.setSink(tmp);

    oper.beginWindow(0);
    Customer tuple1 = new Customer(1, "Anil");
    oper.input1.process(tuple1);
    Order order = new Order(102, 1, 300);
    oper.input2.process(order);
    Order order2 = new Order(103, 1, 300);
    oper.input2.process(order2);
    oper.endWindow();

    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 2, sink.collectedTuples.size());
    CustOrder emitted = sink.collectedTuples.get(0);

    Assert.assertEquals("value of ID :", tuple1.ID, emitted.ID);
    Assert.assertEquals("value of Name :", tuple1.Name, emitted.Name);
    Assert.assertEquals("value of OID: ", order.OID, emitted.OID);
    Assert.assertEquals("value of Amount: ", order.Amount, emitted.Amount);
    emitted = sink.collectedTuples.get(1);
    Assert.assertEquals("value of ID :", tuple1.ID, emitted.ID);
    Assert.assertEquals("value of Name :", tuple1.Name, emitted.Name);
    Assert.assertEquals("value of OID: ", order2.OID, emitted.OID);
    Assert.assertEquals("value of Amount: ", order2.Amount, emitted.Amount);
    oper.teardown();
  }
}
