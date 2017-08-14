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
package org.apache.apex.malhar.contrib.avro;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.helper.TestPortContext;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

public class PojoToAvroTest
{

  private static final String AVRO_SCHEMA = "{\"namespace\":\"abc\"," + ""
      + "\"type\":\"record\",\"doc\":\"Order schema\"," + "\"name\":\"Order\",\"fields\":[{\"name\":\"orderId\","
      + "\"type\": \"long\"}," + "{\"name\":\"customerId\",\"type\": \"int\"},"
      + "{\"name\":\"total\",\"type\": \"double\"}," + "{\"name\":\"customerName\",\"type\": \"string\"}]}";

  CollectorTestSink<Object> outputSink = new CollectorTestSink<>();
  PojoToAvro avroWriter = new PojoToAvro();

  public class TestMeta extends TestWatcher
  {
    public String dir = null;
    Context.OperatorContext context;
    Context.PortContext portContext;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      Attribute.AttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
      portAttributes.put(Context.PortContext.TUPLE_CLASS, SimpleOrder.class);
      portContext = new TestPortContext(portAttributes);
      super.starting(description);
      avroWriter.output.setSink(outputSink);
    }

    @Override
    protected void finished(Description description)
    {
      avroWriter.teardown();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testWriting() throws Exception
  {

    List<SimpleOrder> orderList = new ArrayList<>();
    orderList.add(new SimpleOrder(1, 11, 100.25, "customerOne"));
    orderList.add(new SimpleOrder(2, 22, 200.25, "customerTwo"));
    orderList.add(new SimpleOrder(3, 33, 300.25, "customerThree"));

    avroWriter.setSchemaString(AVRO_SCHEMA);
    avroWriter.data.setup(testMeta.portContext);
    avroWriter.setup(testMeta.context);

    avroWriter.beginWindow(0);

    ListIterator<SimpleOrder> itr = orderList.listIterator();

    while (itr.hasNext()) {
      avroWriter.data.process(itr.next());
    }

    avroWriter.endWindow();
    Assert.assertEquals("Number of tuples", 3, outputSink.collectedTuples.size());
    avroWriter.teardown();

  }

  @Test
  public void testWriteFailure() throws Exception
  {

    List<Order> orderList = new ArrayList<>();
    orderList.add(new Order(11));
    orderList.add(new Order(22));
    orderList.add(new Order(33));

    avroWriter.setSchemaString(AVRO_SCHEMA);

    avroWriter.setup(testMeta.context);
    avroWriter.data.setup(testMeta.portContext);

    avroWriter.beginWindow(0);

    ListIterator<Order> itr = orderList.listIterator();

    while (itr.hasNext()) {
      avroWriter.data.process(itr.next());
    }

    Assert.assertEquals("Field write failures", 12, avroWriter.fieldErrorCount);

    Assert.assertEquals("Record write failures", 3, avroWriter.errorCount);

    avroWriter.endWindow();

    Assert.assertEquals("Number of tuples", 0, outputSink.collectedTuples.size());

    avroWriter.teardown();

  }

  public static class SimpleOrder
  {

    private Integer customerId;
    private Long orderId;
    private Double total;
    private String customerName;

    public SimpleOrder()
    {
    }

    public SimpleOrder(int customerId, long orderId, double total, String customerName)
    {
      setCustomerId(customerId);
      setOrderId(orderId);
      setTotal(total);
      setCustomerName(customerName);
    }

    public String getCustomerName()
    {
      return customerName;
    }

    public void setCustomerName(String customerName)
    {
      this.customerName = customerName;
    }

    public Integer getCustomerId()
    {
      return customerId;
    }

    public void setCustomerId(Integer customerId)
    {
      this.customerId = customerId;
    }

    public Long getOrderId()
    {
      return orderId;
    }

    public void setOrderId(Long orderId)
    {
      this.orderId = orderId;
    }

    public Double getTotal()
    {
      return total;
    }

    public void setTotal(Double total)
    {
      this.total = total;
    }

    @Override
    public String toString()
    {
      return "SimpleOrder [customerId=" + customerId + ", orderId=" + orderId + ", total=" + total + ", customerName="
          + customerName + "]";
    }

  }

  public static class Order
  {

    private int orderId;

    public Order()
    {

    }

    public Order(int orderId)
    {
      this.orderId = orderId;
    }

    public int getOrderId()
    {
      return orderId;
    }

    public void setOrderId(int orderId)
    {
      this.orderId = orderId;
    }

    @Override
    public String toString()
    {
      return "Order [orderId=" + orderId + "]";
    }

  }

}
