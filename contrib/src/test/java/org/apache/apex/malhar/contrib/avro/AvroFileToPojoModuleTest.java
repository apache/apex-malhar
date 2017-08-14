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

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.python.google.common.collect.Lists;

import org.apache.apex.engine.EmbeddedAppLauncherImpl;
import org.apache.apex.malhar.lib.helper.TestPortContext;
import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

/**
 * <p>
 * In this class the emitTuples method is called twice to process the first
 * input, since on begin window 0 the operator is setup & stream is initialized.
 * The platform calls the emitTuples method in the successive windows
 * </p>
 */
public class AvroFileToPojoModuleTest
{

  private static final String AVRO_SCHEMA = "{\"namespace\":\"abc\"," + "" + "\"type\":\"record\",\"doc\":\"Order schema\"," + "\"name\":\"Order\",\"fields\":[{\"name\":\"orderId\"," + "\"type\": \"long\"}," + "{\"name\":\"customerId\",\"type\": \"int\"}," + "{\"name\":\"total\",\"type\": \"double\"}," + "{\"name\":\"customerName\",\"type\": \"string\"}]}";

  private static final String FILENAME = "/tmp/simpleorder.avro";
  private static final String OTHER_FILE = "/tmp/simpleorder2.avro";

  AvroFileToPojoModule avroFileToPojoModule = new AvroFileToPojoModule();

  private List<GenericRecord> recordList = null;

  public static class TestMeta extends TestWatcher
  {
    public String dir = null;
    OperatorContext context;
    PortContext portContext;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
      Attribute.AttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(Context.DAGContext.APPLICATION_PATH, dir);
      context = mockOperatorContext(1, attributes);
      Attribute.AttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
      portAttributes.put(Context.PortContext.TUPLE_CLASS, SimpleOrder.class);
      portContext = new TestPortContext(portAttributes);
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  private void createAvroInput(int cnt)
  {
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

  private void writeAvroFile(File outputFile)
  {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(new Schema.Parser().parse(AVRO_SCHEMA));
    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.create(new Schema.Parser().parse(AVRO_SCHEMA), outputFile);

      for (GenericRecord record : recordList) {
        dataFileWriter.append(record);
      }
      FileUtils.moveFileToDirectory(new File(outputFile.getAbsolutePath()), new File(testMeta.dir), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testAvroToPojoModule() throws Exception
  {
    try {
      FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
      int cnt = 7;
      createAvroInput(cnt);
      writeAvroFile(new File(FILENAME));
      createAvroInput(cnt - 2);
      writeAvroFile(new File(OTHER_FILE));

      avroFileToPojoModule.setAvroFileDirectory(testMeta.dir);
      avroFileToPojoModule.setPojoClass(SimpleOrder.class);

      AvroToPojo avroToPojo = new AvroToPojo();
      avroToPojo.setPojoClass(SimpleOrder.class);

      EmbeddedAppLauncherImpl lma = new EmbeddedAppLauncherImpl();
      Configuration conf = new Configuration(false);

      AvroToPojoApplication avroToPojoApplication = new AvroToPojoApplication();
      avroToPojoApplication.setAvroFileToPojoModule(avroFileToPojoModule);

      lma.prepareDAG(avroToPojoApplication, conf);
      EmbeddedAppLauncherImpl.Controller lc = lma.getController();
      lc.run(10000);// runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  public static class AvroToPojoApplication implements StreamingApplication
  {
    AvroFileToPojoModule avroFileToPojoModule;

    public AvroFileToPojoModule getAvroFileToPojoModule()
    {
      return avroFileToPojoModule;
    }

    public void setAvroFileToPojoModule(AvroFileToPojoModule avroFileToPojoModule)
    {
      this.avroFileToPojoModule = avroFileToPojoModule;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      AvroFileToPojoModule avroFileToPojoModule = dag.addModule("avroFileToPojoModule", getAvroFileToPojoModule());
      ConsoleOutputOperator consoleOutput = dag.addOperator("console", new ConsoleOutputOperator());

      dag.addStream("POJO", avroFileToPojoModule.output, consoleOutput.input);
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
      return "SimpleOrder [customerId=" + customerId + ", orderId=" + orderId + ", total=" + total + ", customerName=" + customerName + "]";
    }
  }
}
