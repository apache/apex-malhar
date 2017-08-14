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
package org.apache.apex.malhar.contrib.parquet;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.PojoUtils;
import org.apache.apex.malhar.lib.util.PojoUtils.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import com.google.common.collect.Sets;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class ParquetFilePOJOReaderTest
{
  private static final String PARQUET_SCHEMA = "message org.apache.apex.malhar.contrib.parquet.eventsEventRecord {"
      + "required INT32 event_id;" + "required BINARY org_id (UTF8);" + "required INT64 long_id;"
      + "optional BOOLEAN css_file_loaded;" + "optional FLOAT float_val;" + "optional DOUBLE double_val;}";

  CollectorTestSink<Object> outputSink = new CollectorTestSink<>();
  ParquetFilePOJOReader parquetFilePOJOReader = new ParquetFilePOJOReader();

  public static class TestMeta extends TestWatcher
  {
    public String dir = null;
    Context.OperatorContext context;
    Context.PortContext portContext;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
      Attribute.AttributeMap operAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
      operAttributes.put(Context.DAGContext.APPLICATION_PATH, dir);
      Attribute.AttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
      portAttributes.put(Context.PortContext.TUPLE_CLASS, EventRecord.class);
      context = mockOperatorContext(1, operAttributes);
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
  public void testParquetReading() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    List<EventRecord> data = Lists.newArrayList();
    data.add(new EventRecord(1, "cust1", 12321L, true, 12.22f, 12.23));
    data.add(new EventRecord(2, "cust2", 12322L, true, 22.22f, 22.23));
    data.add(new EventRecord(3, "cust3", 12323L, true, 32.22f, 32.23));
    writeParquetFile(PARQUET_SCHEMA, new File(testMeta.dir, "data.parquet"), data);

    parquetFilePOJOReader.output.setSink(outputSink);
    parquetFilePOJOReader.setDirectory(testMeta.dir);
    parquetFilePOJOReader.setParquetSchema(PARQUET_SCHEMA);
    parquetFilePOJOReader.setup(testMeta.context);
    parquetFilePOJOReader.output.setup(testMeta.portContext);

    for (long wid = 0; wid < 2; wid++) {
      parquetFilePOJOReader.beginWindow(0);
      parquetFilePOJOReader.emitTuples();
      parquetFilePOJOReader.endWindow();
    }

    Assert.assertEquals("number tuples", 3, outputSink.collectedTuples.size());
    parquetFilePOJOReader.teardown();

  }

  @Test
  public void testParquetReadingWithParquetToPojoMapping() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    List<EventRecord> data = Lists.newArrayList();
    data.add(new EventRecord(1, "cust1", 12321L, true, 12.22f, 12.23));
    data.add(new EventRecord(2, "cust2", 12322L, true, 22.22f, 22.23));
    data.add(new EventRecord(3, "cust3", 12323L, true, 32.22f, 32.23));
    writeParquetFile(PARQUET_SCHEMA, new File(testMeta.dir, "data.parquet"), data);

    parquetFilePOJOReader.output.setSink(outputSink);
    parquetFilePOJOReader.setDirectory(testMeta.dir);
    parquetFilePOJOReader.setParquetSchema(PARQUET_SCHEMA);
    parquetFilePOJOReader.setParquetToPOJOFieldsMapping(
        "event_id:event_id_v2:INTEGER,org_id:org_id_v2:STRING,long_id:long_id_v2:LONG,css_file_loaded:css_file_loaded_v2:BOOLEAN,float_val:float_val_v2:FLOAT,double_val:double_val_v2:DOUBLE");
    parquetFilePOJOReader.setup(testMeta.context);
    testMeta.portContext.getAttributes().put(Context.PortContext.TUPLE_CLASS, EventRecordV2.class);
    parquetFilePOJOReader.output.setup(testMeta.portContext);

    for (long wid = 0; wid < 2; wid++) {
      parquetFilePOJOReader.beginWindow(0);
      parquetFilePOJOReader.emitTuples();
      parquetFilePOJOReader.endWindow();
    }

    Assert.assertEquals("number tuples", 3, outputSink.collectedTuples.size());
    parquetFilePOJOReader.teardown();

  }

  @Test
  public void testParquetEmptyFile() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    List<EventRecord> data = Lists.newArrayList();
    writeParquetFile(PARQUET_SCHEMA, new File(testMeta.dir, "data.parquet"), data);

    parquetFilePOJOReader.output.setSink(outputSink);
    parquetFilePOJOReader.setDirectory(testMeta.dir);
    parquetFilePOJOReader.setParquetSchema(PARQUET_SCHEMA);
    parquetFilePOJOReader.setup(testMeta.context);
    testMeta.portContext.getAttributes().put(Context.PortContext.TUPLE_CLASS, EventRecordV2.class);
    parquetFilePOJOReader.output.setup(testMeta.portContext);

    for (long wid = 0; wid < 2; wid++) {
      parquetFilePOJOReader.beginWindow(0);
      parquetFilePOJOReader.emitTuples();
      parquetFilePOJOReader.endWindow();
    }

    Assert.assertEquals("number tuples", 0, outputSink.collectedTuples.size());
    parquetFilePOJOReader.teardown();

  }

  @Test
  public void testParquetIncorrectFormat() throws Exception
  {
    FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
    List<String> allLines = Lists.newArrayList();
    HashSet<String> lines = Sets.newHashSet();
    for (int line = 0; line < 5; line++) {
      lines.add("f0" + "l" + line);
    }
    allLines.addAll(lines);
    File testFile = new File(testMeta.dir, "file0");
    FileUtils.write(testFile, StringUtils.join(lines, '\n'));

    parquetFilePOJOReader.output.setSink(outputSink);
    parquetFilePOJOReader.setDirectory(testMeta.dir);
    parquetFilePOJOReader.setParquetSchema(PARQUET_SCHEMA);
    parquetFilePOJOReader.setParquetToPOJOFieldsMapping(
        "event_id:event_id_v2:INTEGER,org_id:org_id_v2:STRING,long_id:long_id_v2:LONG,css_file_loaded:css_file_loaded_v2:BOOLEAN,float_val:float_val_v2:FLOAT,double_val:double_val_v2:DOUBLE");
    parquetFilePOJOReader.setup(testMeta.context);
    testMeta.portContext.getAttributes().put(Context.PortContext.TUPLE_CLASS, EventRecordV2.class);
    parquetFilePOJOReader.output.setup(testMeta.portContext);

    for (long wid = 0; wid < 2; wid++) {
      parquetFilePOJOReader.beginWindow(0);
      parquetFilePOJOReader.emitTuples();
      parquetFilePOJOReader.endWindow();
    }

    Assert.assertEquals("number tuples", 0, outputSink.collectedTuples.size());
    parquetFilePOJOReader.teardown();

  }

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
      List<EventRecord> data = Lists.newArrayList();
      data.add(new EventRecord(1, "cust1", 12321L, true, 12.22f, 12.23));
      data.add(new EventRecord(2, "cust2", 12322L, true, 22.22f, 22.23));
      data.add(new EventRecord(3, "cust3", 12323L, true, 32.22f, 32.23));
      writeParquetFile(PARQUET_SCHEMA, new File(testMeta.dir, "data.parquet"), data);
      parquetFilePOJOReader.setDirectory(testMeta.dir);
      parquetFilePOJOReader.setParquetSchema(PARQUET_SCHEMA);
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      ParquetReaderApplication parquetReaderApplication = new ParquetReaderApplication();
      parquetReaderApplication.setParquetFilePOJOReader(parquetFilePOJOReader);
      lma.prepareDAG(parquetReaderApplication, conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000);// runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private static void writeParquetFile(String rawSchema, File outputParquetFile, List<EventRecord> data)
    throws IOException
  {
    Path path = new Path(outputParquetFile.toURI());
    MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
    ParquetPOJOWriter writer = new ParquetPOJOWriter(path, schema, EventRecord.class, true);
    for (EventRecord eventRecord : data) {
      writer.write(eventRecord);
    }
    writer.close();
  }

  public static class ParquetReaderApplication implements StreamingApplication
  {

    ParquetFilePOJOReader parquetFilePOJOReader;

    public ParquetFilePOJOReader getParquetFilePOJOReader()
    {
      return parquetFilePOJOReader;
    }

    public void setParquetFilePOJOReader(ParquetFilePOJOReader parquetFilePOJOReader)
    {
      this.parquetFilePOJOReader = parquetFilePOJOReader;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      ParquetFilePOJOReader parquetReader = dag.addOperator("parquetReader", getParquetFilePOJOReader());
      ConsoleOutputOperator pojoOp = dag.addOperator("pojoOp", new ConsoleOutputOperator());
      dag.getMeta(parquetReader).getMeta(parquetReader.output).getAttributes().put(Context.PortContext.TUPLE_CLASS,
          EventRecord.class);
      dag.addStream("pojo", parquetReader.output, pojoOp.input);
    }

  }

  public static class EventRecord
  {

    private String org_id;
    private int event_id;
    private long long_id;
    private boolean css_file_loaded;
    private float float_val;
    private double double_val;
    private String extra_field;

    public EventRecord()
    {
    }

    public EventRecord(int event_id, String org_id, long long_id, boolean css_file_loaded, float float_val,
        double double_val)
    {
      this.org_id = org_id;
      this.event_id = event_id;
      this.long_id = long_id;
      this.css_file_loaded = css_file_loaded;
      this.float_val = float_val;
      this.double_val = double_val;
    }

    public String getOrg_id()
    {
      return org_id;
    }

    public void setOrg_id(String org_id)
    {
      this.org_id = org_id;
    }

    public int getEvent_id()
    {
      return event_id;
    }

    public void setEvent_id(int event_id)
    {
      this.event_id = event_id;
    }

    public long getLong_id()
    {
      return long_id;
    }

    public void setLong_id(long long_id)
    {
      this.long_id = long_id;
    }

    public boolean isCss_file_loaded()
    {
      return css_file_loaded;
    }

    public void setCss_file_loaded(boolean css_file_loaded)
    {
      this.css_file_loaded = css_file_loaded;
    }

    public float getFloat_val()
    {
      return float_val;
    }

    public void setFloat_val(float float_val)
    {
      this.float_val = float_val;
    }

    public double getDouble_val()
    {
      return double_val;
    }

    public void setDouble_val(double double_val)
    {
      this.double_val = double_val;
    }

    public String getExtra_field()
    {
      return extra_field;
    }

    public void setExtra_field(String extra_field)
    {
      this.extra_field = extra_field;
    }

    @Override
    public String toString()
    {
      return "EventRecord [org_id=" + org_id + ", event_id=" + event_id + ", long_id=" + long_id + ", css_file_loaded="
          + css_file_loaded + ", float_val=" + float_val + ", double_val=" + double_val + "]";
    }
  }

  public static class EventRecordV2
  {

    private String org_id_v2;
    private int event_id_v2;
    private long long_id_v2;
    private boolean css_file_loaded_v2;
    private float float_val_v2;
    private double double_val_v2;

    public EventRecordV2()
    {
    }

    public EventRecordV2(int event_id_v2, String org_id_v2, long long_id_v2, boolean css_file_loaded_v2,
        float float_val_v2, double double_val_v2)
    {
      this.org_id_v2 = org_id_v2;
      this.event_id_v2 = event_id_v2;
      this.long_id_v2 = long_id_v2;
      this.css_file_loaded_v2 = css_file_loaded_v2;
      this.float_val_v2 = float_val_v2;
      this.double_val_v2 = double_val_v2;
    }

    public String getOrg_id_v2()
    {
      return org_id_v2;
    }

    public void setOrg_id_v2(String org_id_v2)
    {
      this.org_id_v2 = org_id_v2;
    }

    public int getEvent_id_v2()
    {
      return event_id_v2;
    }

    public void setEvent_id_v2(int event_id_v2)
    {
      this.event_id_v2 = event_id_v2;
    }

    public long getLong_id_v2()
    {
      return long_id_v2;
    }

    public void setLong_id_v2(long long_id_v2)
    {
      this.long_id_v2 = long_id_v2;
    }

    public boolean isCss_file_loaded_v2()
    {
      return css_file_loaded_v2;
    }

    public void setCss_file_loaded_v2(boolean css_file_loaded_v2)
    {
      this.css_file_loaded_v2 = css_file_loaded_v2;
    }

    public float getFloat_val_v2()
    {
      return float_val_v2;
    }

    public void setFloat_val_v2(float float_val_v2)
    {
      this.float_val_v2 = float_val_v2;
    }

    public double getDouble_val_v2()
    {
      return double_val_v2;
    }

    public void setDouble_val_v2(double double_val_v2)
    {
      this.double_val_v2 = double_val_v2;
    }

    @Override
    public String toString()
    {
      return "EventRecordV2 [org_id_v2=" + org_id_v2 + ", event_id_v2=" + event_id_v2 + ", long_id_v2=" + long_id_v2
          + ", css_file_loaded_v2=" + css_file_loaded_v2 + ", float_val_v2=" + float_val_v2 + ", double_val_v2="
          + double_val_v2 + "]";
    }

  }

  public static class ParquetPOJOWriter extends ParquetWriter<Object>
  {

    Class<?> klass;

    public ParquetPOJOWriter(Path file, MessageType schema, Class klass) throws IOException
    {
      this(file, schema, klass, false);
    }

    public ParquetPOJOWriter(Path file, MessageType schema, Class klass, boolean enableDictionary) throws IOException
    {
      this(file, schema, klass, CompressionCodecName.UNCOMPRESSED, enableDictionary);
    }

    public ParquetPOJOWriter(Path file, MessageType schema, Class klass, CompressionCodecName codecName,
        boolean enableDictionary) throws IOException
    {
      super(file, new POJOWriteSupport(schema, klass), codecName, DEFAULT_BLOCK_SIZE,
          DEFAULT_PAGE_SIZE, enableDictionary, false);
    }

  }

  public static class POJOWriteSupport extends WriteSupport<Object>
  {
    MessageType schema;
    RecordConsumer recordConsumer;
    List<ColumnDescriptor> cols;
    Class<?> klass;
    private List<Getter> keyMethodMap;

    public POJOWriteSupport(MessageType schema, Class<?> klass)
    {
      this.schema = schema;
      this.cols = schema.getColumns();
      this.klass = klass;
      init();
    }

    private void init()
    {
      keyMethodMap = new ArrayList<>();
      for (int i = 0; i < cols.size(); i++) {
        try {
          keyMethodMap.add(generateGettersForField(klass, cols.get(i).getPath()[0]));
        } catch (NoSuchFieldException | SecurityException e) {
          throw new RuntimeException("Failed to initialize pojo class getters for field: ", e);
        }
      }

    }

    @Override
    public WriteContext init(Configuration configuration)
    {
      return new WriteContext(schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer)
    {
      this.recordConsumer = recordConsumer;

    }

    @Override
    public void write(Object record)
    {
      recordConsumer.startMessage();
      for (int i = 0; i < cols.size(); ++i) {
        String val = keyMethodMap.get(i).get(record).toString();
        recordConsumer.startField(cols.get(i).getPath()[0], i);
        switch (cols.get(i).getType()) {
          case BOOLEAN:
            recordConsumer.addBoolean(Boolean.parseBoolean(val));
            break;
          case FLOAT:
            recordConsumer.addFloat(Float.parseFloat(val));
            break;
          case DOUBLE:
            recordConsumer.addDouble(Double.parseDouble(val));
            break;
          case INT32:
            recordConsumer.addInteger(Integer.parseInt(val));
            break;
          case INT64:
            recordConsumer.addLong(Long.parseLong(val));
            break;
          case BINARY:
            recordConsumer.addBinary(stringToBinary(val));
            break;
          default:
            throw new ParquetEncodingException("Unsupported column type: " + cols.get(i).getType());
        }
        recordConsumer.endField(cols.get(i).getPath()[0], i);
      }
      recordConsumer.endMessage();
    }

    private Binary stringToBinary(Object value)
    {
      return Binary.fromString(value.toString());
    }

    private Getter generateGettersForField(Class<?> klass, String inputFieldName)
      throws NoSuchFieldException, SecurityException
    {
      Field f = klass.getDeclaredField(inputFieldName);
      Class c = ClassUtils.primitiveToWrapper(f.getType());

      Getter classGetter = PojoUtils.createGetter(klass, inputFieldName, c);
      return classGetter;
    }
  }

}
