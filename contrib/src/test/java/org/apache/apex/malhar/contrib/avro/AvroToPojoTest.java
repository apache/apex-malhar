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

import java.util.List;
import java.util.ListIterator;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.python.google.common.collect.Lists;

import org.apache.apex.malhar.lib.helper.TestPortContext;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;

public class AvroToPojoTest
{
  public static final String fieldInfoInitMap = "orderId:orderId:LONG," + "customerId:customerId:INTEGER,"
      + "customerName:customerName:STRING," + "total:total:DOUBLE";

  public static final String byteFieldInfoInitMap = "orderId:orderId:LONG," + "customerId:customerId:INTEGER,"
      + "customerName:customerName:BYTES," + "total:total:DOUBLE";

  private static final String AVRO_SCHEMA = "{\"namespace\":\"abc\"," + ""
      + "\"type\":\"record\",\"doc\":\"Order schema\"," + "\"name\":\"Order\",\"fields\":[{\"name\":\"orderId\","
      + "\"type\": \"long\"}," + "{\"name\":\"customerId\",\"type\": \"int\"},"
      + "{\"name\":\"total\",\"type\": \"double\"}," + "{\"name\":\"customerName\",\"type\": \"string\"}]}";

  private static final String AVRO_SCHEMA_FOR_BYTES = "{\"namespace\":\"abc\"," + ""
      + "\"type\":\"record\",\"doc\":\"Order schema\"," + "\"name\":\"Order\",\"fields\":[{\"name\":\"orderId\","
      + "\"type\": \"long\"}," + "{\"name\":\"customerId\",\"type\": \"int\"},"
      + "{\"name\":\"total\",\"type\": \"double\"}," + "{\"name\":\"customerName\",\"type\": \"bytes\"}]}";

  CollectorTestSink<Object> outputSink = new CollectorTestSink<Object>();
  AvroToPojo avroReader = new AvroToPojo();

  private List<GenericRecord> recordList = null;

  public class TestMeta extends TestWatcher
  {
    Context.OperatorContext context;
    Context.PortContext portContext;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      Attribute.AttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
      portAttributes.put(Context.PortContext.TUPLE_CLASS, SimpleOrder.class);
      portContext = new TestPortContext(portAttributes);
      super.starting(description);
      avroReader.output.setSink(outputSink);
      createReaderInput();
    }

    @Override
    protected void finished(Description description)
    {
      avroReader.teardown();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testAvroReads() throws Exception
  {

    avroReader.setPojoClass(SimpleOrder.class);
    avroReader.setGenericRecordToPOJOFieldsMapping(fieldInfoInitMap);
    avroReader.output.setup(testMeta.portContext);
    avroReader.setup(testMeta.context);

    avroReader.beginWindow(0);

    ListIterator<GenericRecord> itr = recordList.listIterator();

    while (itr.hasNext()) {
      avroReader.data.process(itr.next());
    }

    avroReader.endWindow();
    Assert.assertEquals("Number of tuples", 3, outputSink.collectedTuples.size());
    avroReader.teardown();

  }

  @Test
  public void testAvroReadsInvalidDataType() throws Exception
  {

    avroReader.setPojoClass(SimpleOrder.class);
    avroReader.setGenericRecordToPOJOFieldsMapping(byteFieldInfoInitMap);
    avroReader.output.setup(testMeta.portContext);
    avroReader.setup(testMeta.context);

    avroReader.beginWindow(0);

    ListIterator<GenericRecord> itr = recordList.listIterator();

    while (itr.hasNext()) {
      avroReader.data.process(itr.next());
    }

    avroReader.endWindow();
    Assert.assertEquals("Number of tuples", 3, outputSink.collectedTuples.size());
    avroReader.teardown();

  }

  @Test
  public void testAvroReadsWithReflection() throws Exception
  {

    avroReader.setPojoClass(SimpleOrder.class);
    avroReader.output.setup(testMeta.portContext);
    avroReader.setup(testMeta.context);

    avroReader.beginWindow(0);

    ListIterator<GenericRecord> itr = recordList.listIterator();

    while (itr.hasNext()) {
      avroReader.data.process(itr.next());
    }

    avroReader.endWindow();
    Assert.assertEquals("Number of tuples", 3, outputSink.collectedTuples.size());
    avroReader.teardown();

  }

  @Test
  public void testReadFailures() throws Exception
  {

    avroReader.setPojoClass(SimpleOrder.class);
    avroReader.setGenericRecordToPOJOFieldsMapping(fieldInfoInitMap);
    avroReader.output.setup(testMeta.portContext);
    avroReader.setup(testMeta.context);

    avroReader.beginWindow(0);

    ListIterator<GenericRecord> itr = recordList.listIterator();

    while (itr.hasNext()) {
      GenericRecord rec = itr.next();
      rec.put("orderId", "abc");
      avroReader.data.process(rec);
    }

    Assert.assertEquals("Number of tuples", 3, avroReader.errorCount);
    avroReader.endWindow();
    avroReader.teardown();

  }

  @Test
  public void testReadFieldFailures() throws Exception
  {

    int cnt = 3;

    avroReader.setPojoClass(SimpleOrder.class);
    avroReader.setGenericRecordToPOJOFieldsMapping(fieldInfoInitMap);
    avroReader.output.setup(testMeta.portContext);
    avroReader.setup(testMeta.context);

    avroReader.beginWindow(0);

    for (int i = 0; i < cnt; i++) {
      avroReader.data.process(null);
    }

    Assert.assertEquals("Number of tuples", 12, avroReader.fieldErrorCount);

    avroReader.endWindow();
    avroReader.teardown();

  }

  private void createReaderInput()
  {
    int cnt = 3;

    recordList = Lists.newArrayList();

    while (cnt > 0) {
      GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(AVRO_SCHEMA));
      rec.put("orderId", cnt * 1L);
      rec.put("customerId", cnt * 2);
      rec.put("total", cnt * 1.5);
      rec.put("customerName", "*" + cnt + "*");
      cnt--;
      recordList.add(rec);
    }
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
