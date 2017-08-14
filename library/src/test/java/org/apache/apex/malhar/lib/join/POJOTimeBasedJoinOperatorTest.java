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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;

import com.esotericsoftware.kryo.Kryo;

public class POJOTimeBasedJoinOperatorTest
{

  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  public class Customer
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

  public class Order
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

  @Test
  public void testInnerJoinOperator() throws IOException, InterruptedException
  {
    Kryo kryo = new Kryo();
    POJOJoinOperator oper = new POJOJoinOperator();
    JoinStore store = new InMemoryStore(200, 200);
    oper.setLeftStore(kryo.copy(store));
    oper.setRightStore(kryo.copy(store));
    oper.setIncludeFields("ID,Name;OID,Amount");
    oper.setKeyFields("ID,CID");
    oper.outputClass = CustOrder.class;

    oper.setup(MapTimeBasedJoinOperator.context);

    CollectorTestSink<List<CustOrder>> sink = new CollectorTestSink<List<CustOrder>>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink)sink;
    oper.outputPort.setSink(tmp);

    oper.beginWindow(0);

    Customer tuple = new Customer(1, "Anil");

    oper.input1.process(tuple);

    CountDownLatch latch = new CountDownLatch(1);

    Order order = new Order(102, 1, 300);

    oper.input2.process(order);

    Order order2 = new Order(103, 3, 300);
    oper.input2.process(order2);

    Order order3 = new Order(104, 7, 300);
    oper.input2.process(order3);

    latch.await(3000, TimeUnit.MILLISECONDS);

    oper.endWindow();

    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 1, sink.collectedTuples.size());
    List<CustOrder> emittedList = sink.collectedTuples.iterator().next();
    CustOrder emitted = emittedList.get(0);

    Assert.assertEquals("value of ID :", tuple.ID, emitted.ID);
    Assert.assertEquals("value of Name :", tuple.Name, emitted.Name);

    Assert.assertEquals("value of OID: ", order.OID, emitted.OID);
    Assert.assertEquals("value of Amount: ", order.Amount, emitted.Amount);

  }

  @Test
  public void testLeftOuterJoinOperator() throws IOException, InterruptedException
  {
    Kryo kryo = new Kryo();
    POJOJoinOperator oper = new POJOJoinOperator();
    JoinStore store = new InMemoryStore(200, 200);
    oper.setLeftStore(kryo.copy(store));
    oper.setRightStore(kryo.copy(store));
    oper.setIncludeFields("ID,Name;OID,Amount");
    oper.setKeyFields("ID,CID");
    oper.outputClass = CustOrder.class;
    oper.setStrategy("left_outer_join");

    oper.setup(MapTimeBasedJoinOperator.context);

    CollectorTestSink<List<CustOrder>> sink = new CollectorTestSink<List<CustOrder>>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink)sink;
    oper.outputPort.setSink(tmp);

    oper.beginWindow(0);

    Customer tuple1 = new Customer(1, "Anil");

    oper.input1.process(tuple1);

    CountDownLatch latch = new CountDownLatch(1);

    Order order = new Order(102, 3, 300);

    oper.input2.process(order);

    Order order2 = new Order(103, 7, 300);
    oper.input2.process(order2);

    oper.endWindow();

    latch.await(500, TimeUnit.MILLISECONDS);

    oper.beginWindow(1);
    Order order3 = new Order(104, 5, 300);
    oper.input2.process(order3);

    Customer tuple2 = new Customer(5, "DT");

    oper.input1.process(tuple2);

    latch.await(500, TimeUnit.MILLISECONDS);

    oper.endWindow();
    latch.await(500, TimeUnit.MILLISECONDS);
    oper.beginWindow(2);
    oper.endWindow();
    latch.await(5000, TimeUnit.MILLISECONDS);


    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 2, sink.collectedTuples.size());
    Iterator<List<CustOrder>> ite = sink.collectedTuples.iterator();
    List<CustOrder> emittedList = ite.next();
    CustOrder emitted = emittedList.get(0);

    Assert.assertEquals("value of ID :", tuple2.ID, emitted.ID);
    Assert.assertEquals("value of Name :", tuple2.Name, emitted.Name);

    Assert.assertEquals("value of OID: ", order3.OID, emitted.OID);
    Assert.assertEquals("value of Amount: ", order3.Amount, emitted.Amount);

    emittedList = ite.next();
    emitted = emittedList.get(0);
    Assert.assertEquals("Joined Tuple ", "{ID=1, Name='Anil', OID=0, Amount=0}", emitted.toString());
  }

  @Test
  public void testRightOuterJoinOperator() throws IOException, InterruptedException
  {
    Kryo kryo = new Kryo();
    POJOJoinOperator oper = new POJOJoinOperator();
    JoinStore store = new InMemoryStore(200, 200);
    oper.setLeftStore(kryo.copy(store));
    oper.setRightStore(kryo.copy(store));
    oper.setIncludeFields("ID,Name;OID,Amount");
    oper.setKeyFields("ID,CID");
    oper.outputClass = CustOrder.class;
    oper.setStrategy("right_outer_join");

    oper.setup(MapTimeBasedJoinOperator.context);

    CollectorTestSink<List<CustOrder>> sink = new CollectorTestSink<List<CustOrder>>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink)sink;
    oper.outputPort.setSink(tmp);

    oper.beginWindow(0);

    Customer tuple1 = new Customer(1, "Anil");

    oper.input1.process(tuple1);

    CountDownLatch latch = new CountDownLatch(1);

    Order order = new Order(102, 3, 300);

    oper.input2.process(order);

    Order order2 = new Order(103, 7, 300);
    oper.input2.process(order2);
    oper.endWindow();

    latch.await(500, TimeUnit.MILLISECONDS);

    oper.beginWindow(1);
    Order order3 = new Order(104, 5, 300);
    oper.input2.process(order3);

    Customer tuple2 = new Customer(5, "DT");
    oper.input1.process(tuple2);

    latch.await(500, TimeUnit.MILLISECONDS);

    oper.endWindow();
    latch.await(500, TimeUnit.MILLISECONDS);
    oper.beginWindow(2);
    oper.endWindow();
    latch.await(5000, TimeUnit.MILLISECONDS);

    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 2, sink.collectedTuples.size());
    Iterator<List<CustOrder>> ite = sink.collectedTuples.iterator();
    List<CustOrder> emittedList = ite.next();
    CustOrder emitted = emittedList.get(0);

    Assert.assertEquals("value of ID :", tuple2.ID, emitted.ID);
    Assert.assertEquals("value of Name :", tuple2.Name, emitted.Name);

    Assert.assertEquals("value of OID: ", order3.OID, emitted.OID);
    Assert.assertEquals("value of Amount: ", order3.Amount, emitted.Amount);

    emittedList = ite.next();
    Assert.assertEquals("Joined Tuple ", "{ID=0, Name='null', OID=102, Amount=300}", emittedList.get(0).toString());
    Assert.assertEquals("Joined Tuple ", "{ID=0, Name='null', OID=103, Amount=300}", emittedList.get(1).toString());
  }

  @Test
  public void testFullOuterJoinOperator() throws IOException, InterruptedException
  {
    Kryo kryo = new Kryo();
    POJOJoinOperator oper = new POJOJoinOperator();
    JoinStore store = new InMemoryStore(200, 200);
    oper.setLeftStore(kryo.copy(store));
    oper.setRightStore(kryo.copy(store));
    oper.setIncludeFields("ID,Name;OID,Amount");
    oper.setKeyFields("ID,CID");
    oper.outputClass = CustOrder.class;
    oper.setStrategy("outer_join");

    oper.setup(MapTimeBasedJoinOperator.context);

    CollectorTestSink<List<CustOrder>> sink = new CollectorTestSink<List<CustOrder>>();
    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorTestSink<Object> tmp = (CollectorTestSink)sink;
    oper.outputPort.setSink(tmp);

    oper.beginWindow(0);

    Customer tuple1 = new Customer(1, "Anil");

    oper.input1.process(tuple1);

    CountDownLatch latch = new CountDownLatch(1);

    Order order = new Order(102, 3, 300);

    oper.input2.process(order);

    Order order2 = new Order(103, 7, 300);
    oper.input2.process(order2);
    oper.endWindow();

    latch.await(500, TimeUnit.MILLISECONDS);

    oper.beginWindow(1);
    Order order3 = new Order(104, 5, 300);
    oper.input2.process(order3);

    Customer tuple2 = new Customer(5, "DT");
    oper.input1.process(tuple2);

    latch.await(500, TimeUnit.MILLISECONDS);

    oper.endWindow();
    latch.await(500, TimeUnit.MILLISECONDS);
    oper.beginWindow(2);
    oper.endWindow();
    latch.await(5000, TimeUnit.MILLISECONDS);

    /* Number of tuple, emitted */
    Assert.assertEquals("Number of tuple emitted ", 3, sink.collectedTuples.size());
    Iterator<List<CustOrder>> ite = sink.collectedTuples.iterator();
    List<CustOrder> emittedList = ite.next();
    CustOrder emitted = emittedList.get(0);

    Assert.assertEquals("value of ID :", tuple2.ID, emitted.ID);
    Assert.assertEquals("value of Name :", tuple2.Name, emitted.Name);

    Assert.assertEquals("value of OID: ", order3.OID, emitted.OID);
    Assert.assertEquals("value of Amount: ", order3.Amount, emitted.Amount);

    emittedList = ite.next();
    Assert.assertEquals("Joined Tuple ", "{ID=1, Name='Anil', OID=0, Amount=0}", emittedList.get(0).toString());

    emittedList = ite.next();
    Assert.assertEquals("Joined Tuple ", "{ID=0, Name='null', OID=102, Amount=300}", emittedList.get(0).toString());
    Assert.assertEquals("Joined Tuple ", "{ID=0, Name='null', OID=103, Amount=300}", emittedList.get(1).toString());
  }

}
