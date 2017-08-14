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
import java.util.HashSet;
import java.util.List;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.python.google.common.collect.Lists;

import org.apache.apex.malhar.lib.helper.TestPortContext;
import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.io.fs.AbstractFileInputOperatorTest;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Sink;
import com.datatorrent.api.StreamingApplication;

/**
 * <p>
 * In this class the emitTuples method is called twice to process the first
 * input, since on begin window 0 the operator is setup & stream is initialized.
 * The platform calls the emitTuples method in the successive windows
 * </p>
 */
public class AvroFileInputOperatorTest
{

  private static final String AVRO_SCHEMA = "{\"namespace\":\"abc\"," + ""
      + "\"type\":\"record\",\"doc\":\"Order schema\"," + "\"name\":\"Order\",\"fields\":[{\"name\":\"orderId\","
      + "\"type\": \"long\"}," + "{\"name\":\"customerId\",\"type\": \"int\"},"
      + "{\"name\":\"total\",\"type\": \"double\"}," + "{\"name\":\"customerName\",\"type\": \"string\"}]}";

  private static final String FILENAME = "/tmp/simpleorder.avro";
  private static final String OTHER_FILE = "/tmp/simpleorder2.avro";
  private static final String ERROR_FILE = "/tmp/errorFile.avro";

  CollectorTestSink<Object> output = new CollectorTestSink<Object>();

  CollectorTestSink<Object> completedFilesPort = new CollectorTestSink<Object>();

  CollectorTestSink<Object> errorRecordsPort = new CollectorTestSink<Object>();

  AvroFileInputOperator avroFileInput = new AvroFileInputOperator();

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

  @Test
  public void testSingleFileAvroReads() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);

    int cnt = 7;
    createAvroInput(cnt);
    writeAvroFile(new File(FILENAME));

    avroFileInput.output.setSink(output);
    avroFileInput.completedFilesPort.setSink(completedFilesPort);
    avroFileInput.errorRecordsPort.setSink(errorRecordsPort);
    avroFileInput.setDirectory(testMeta.dir);
    avroFileInput.setup(testMeta.context);

    avroFileInput.beginWindow(0);
    avroFileInput.emitTuples();
    avroFileInput.emitTuples();
    Assert.assertEquals("Record count", cnt, avroFileInput.recordCount);
    avroFileInput.endWindow();
    Assert.assertEquals("number tuples", cnt, output.collectedTuples.size());
    Assert.assertEquals("Error tuples", 0, errorRecordsPort.collectedTuples.size());
    Assert.assertEquals("Completed File", 1, completedFilesPort.collectedTuples.size());
    avroFileInput.teardown();

  }

  @Test
  public void testIdempotencyWithCheckPoint() throws Exception
  {
    AbstractFileInputOperatorTest.testIdempotencyWithCheckPoint(new AvroFileInputOperator(), new CollectorTestSink<String>(), new AbstractFileInputOperatorTest.IdempotencyTestDriver<AvroFileInputOperator>()
    {
      @Override
      public void writeFile(int count, String fileName) throws IOException
      {
        recordList = Lists.newArrayList();

        while (count > 0) {
          GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(AVRO_SCHEMA));
          rec.put("orderId", count * 1L);
          rec.put("customerId", count * 2);
          rec.put("total", count * 1.5);
          rec.put("customerName", "*" + count + "*");
          count--;
          recordList.add(rec);
        }

        writeAvroFile(new File(fileName));
      }

      @Override
      public void setSink(AvroFileInputOperator operator, Sink<?> sink)
      {
        TestUtils.setSink(operator.output, sink);
      }

      @Override
      public String getDirectory()
      {
        return testMeta.dir;
      }

      @Override
      public OperatorContext getContext()
      {
        return testMeta.context;
      }
    });
  }


  @Test
  public void testMultipleFileAvroReads() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);

    int cnt = 7;

    createAvroInput(cnt);

    writeAvroFile(new File(FILENAME));
    writeAvroFile(new File(OTHER_FILE));

    avroFileInput.output.setSink(output);
    avroFileInput.completedFilesPort.setSink(completedFilesPort);
    avroFileInput.errorRecordsPort.setSink(errorRecordsPort);
    avroFileInput.setDirectory(testMeta.dir);
    avroFileInput.setup(testMeta.context);

    avroFileInput.beginWindow(0);
    avroFileInput.emitTuples();
    avroFileInput.beginWindow(1);
    avroFileInput.emitTuples();

    Assert.assertEquals("number tuples after window 0", cnt, output.collectedTuples.size());

    avroFileInput.emitTuples();
    avroFileInput.endWindow();

    Assert.assertEquals("Error tuples", 0, errorRecordsPort.collectedTuples.size());
    Assert.assertEquals("number tuples after window 1", 2 * cnt, output.collectedTuples.size());
    Assert.assertEquals("Completed File", 2, completedFilesPort.collectedTuples.size());

    avroFileInput.teardown();

  }

  @Test
  public void testInvalidFormatFailure() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);

    int cnt = 7;
    writeErrorFile(cnt, new File(ERROR_FILE));

    avroFileInput.output.setSink(output);
    avroFileInput.setDirectory(testMeta.dir);
    avroFileInput.setup(testMeta.context);

    avroFileInput.beginWindow(0);
    avroFileInput.emitTuples();
    avroFileInput.emitTuples();
    avroFileInput.endWindow();

    Assert.assertEquals("number tuples after window 1", 0, output.collectedTuples.size());
    avroFileInput.teardown();
  }

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

  private void writeErrorFile(int cnt, File errorFile) throws IOException
  {
    List<String> allLines = Lists.newArrayList();
    HashSet<String> lines = Sets.newHashSet();
    for (int line = 0; line < 5; line++) {
      lines.add("f0" + "l" + line);
    }

    allLines.addAll(lines);

    FileUtils.write(errorFile, StringUtils.join(lines, '\n'));

    FileUtils.moveFileToDirectory(new File(errorFile.getAbsolutePath()), new File(testMeta.dir), true);
  }

  private void writeAvroFile(File outputFile) throws IOException
  {

    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
        new Schema.Parser().parse(AVRO_SCHEMA));

    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(new Schema.Parser().parse(AVRO_SCHEMA), outputFile);

    for (GenericRecord record : recordList) {
      dataFileWriter.append(record);
    }

    dataFileWriter.close();

    FileUtils.moveFileToDirectory(new File(outputFile.getAbsolutePath()), new File(testMeta.dir), true);

  }

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
      int cnt = 7;
      createAvroInput(cnt);
      writeAvroFile(new File(FILENAME));
      createAvroInput(cnt - 2);
      writeAvroFile(new File(OTHER_FILE));
      avroFileInput.setDirectory(testMeta.dir);

      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);

      AvroReaderApplication avroReaderApplication = new AvroReaderApplication();
      avroReaderApplication.setAvroFileInputOperator(avroFileInput);
      lma.prepareDAG(avroReaderApplication, conf);

      LocalMode.Controller lc = lma.getController();
      lc.run(10000);// runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  @Test
  public void testApplicationWithPojoConversion() throws IOException, Exception
  {
    try {
      FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
      int cnt = 7;
      createAvroInput(cnt);
      writeAvroFile(new File(FILENAME));
      createAvroInput(cnt - 2);
      writeAvroFile(new File(OTHER_FILE));

      avroFileInput.setDirectory(testMeta.dir);

      AvroToPojo avroToPojo = new AvroToPojo();
      avroToPojo.setPojoClass(SimpleOrder.class);

      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);

      AvroToPojoApplication avroToPojoApplication = new AvroToPojoApplication();
      avroToPojoApplication.setAvroFileInputOperator(avroFileInput);
      avroToPojoApplication.setAvroToPojo(avroToPojo);

      lma.prepareDAG(avroToPojoApplication, conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000);// runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  public static class AvroReaderApplication implements StreamingApplication
  {

    AvroFileInputOperator avroFileInputOperator;

    public AvroFileInputOperator getAvroFileInput()
    {
      return avroFileInputOperator;
    }

    public void setAvroFileInputOperator(AvroFileInputOperator avroFileInputOperator)
    {
      this.avroFileInputOperator = avroFileInputOperator;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      AvroFileInputOperator avroInputOperator = dag.addOperator("avroInputOperator", getAvroFileInput());
      ConsoleOutputOperator consoleOutput = dag.addOperator("GenericRecordOp", new ConsoleOutputOperator());
      dag.addStream("pojo", avroInputOperator.output, consoleOutput.input).setLocality(Locality.CONTAINER_LOCAL);
    }

  }

  public static class AvroToPojoApplication implements StreamingApplication
  {

    AvroFileInputOperator avroFileInputOperator;
    AvroToPojo avroToPojo;

    public AvroFileInputOperator getAvroFileInput()
    {
      return avroFileInputOperator;
    }

    public void setAvroFileInputOperator(AvroFileInputOperator avroFileInputOperator)
    {
      this.avroFileInputOperator = avroFileInputOperator;
    }

    public void setAvroToPojo(AvroToPojo avroToPojo)
    {
      this.avroToPojo = avroToPojo;
    }

    public AvroToPojo getAvroToPojo()
    {
      return avroToPojo;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      AvroFileInputOperator avroInputOperator = dag.addOperator("avroInputOperator", getAvroFileInput());
      AvroToPojo avroToPojo = dag.addOperator("AvroToPojo", getAvroToPojo());
      ConsoleOutputOperator consoleOutput = dag.addOperator("GenericRecordOp", new ConsoleOutputOperator());
      dag.getMeta(avroToPojo).getMeta(avroToPojo.output).getAttributes().put(Context.PortContext.TUPLE_CLASS,
          SimpleOrder.class);

      dag.addStream("GenericRecords", avroInputOperator.output, avroToPojo.data).setLocality(Locality.THREAD_LOCAL);
      dag.addStream("POJO", avroToPojo.output, consoleOutput.input).setLocality(Locality.CONTAINER_LOCAL);
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

}
