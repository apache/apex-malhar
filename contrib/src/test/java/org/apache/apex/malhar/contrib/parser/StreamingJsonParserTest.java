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
package org.apache.apex.malhar.contrib.parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ListIterator;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.python.google.common.collect.Lists;

import org.apache.apex.malhar.lib.helper.TestPortContext;
import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.io.fs.AbstractFileInputOperator;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

public class StreamingJsonParserTest
{
  public static final String fieldInfoInitMap = "id:id:INTEGER," + "name:name:STRING," + "gpa:gpa:DOUBLE";

  public static final String nestedFieldInfoMap = "id:id:INTEGER," + "name:name:STRING," + "gpa:gpa:DOUBLE,"
      + "streetAddress:streetAddress:STRING," + "city:city:STRING," + "state:state:STRING,"
      + "postalCode:postalCode:STRING";

  public static final String invalidFieldInfoMap = "Field1:id:INTEGER," + "name:name:STRING," + "gpa:gpa:DOUBLE";

  private static final String FILENAME = "/tmp/streaming.json";

  CollectorTestSink<Object> outputSink = new CollectorTestSink<Object>();
  CollectorTestSink<Object> errorSink = new CollectorTestSink<Object>();

  StreamingJsonParser jsonParser = new StreamingJsonParser();

  private List<String> recordList = null;

  public class TestMeta extends TestWatcher
  {
    Context.OperatorContext context;
    Context.PortContext portContext;
    public String dir = null;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();
      this.dir = "target/" + className + "/" + methodName;
      Attribute.AttributeMap portAttributes = new Attribute.AttributeMap.DefaultAttributeMap();
      portAttributes.put(Context.PortContext.TUPLE_CLASS, Person.class);
      portContext = new TestPortContext(portAttributes);
      super.starting(description);
      jsonParser.output.setup(testMeta.portContext);
      jsonParser.output.setSink(outputSink);
      jsonParser.err.setSink(errorSink);
    }

    @Override
    protected void finished(Description description)
    {
      try {
        FileUtils.deleteDirectory(new File("target/" + description.getClassName()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      jsonParser.teardown();
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testReads() throws Exception
  {
    int count = 5;
    createReaderInput(count);
    jsonParser.setFieldMappingString(fieldInfoInitMap);
    jsonParser.setup(testMeta.context);
    jsonParser.output.setup(testMeta.portContext);

    jsonParser.beginWindow(0);
    ListIterator<String> itr = recordList.listIterator();
    while (itr.hasNext()) {
      jsonParser.in.process(itr.next().getBytes());
    }
    jsonParser.endWindow();

    Assert.assertEquals("Number of tuples", count, outputSink.collectedTuples.size());
    Person obj = (Person)outputSink.collectedTuples.get(0);
    Assert.assertEquals("Name is", "name-5", obj.getName());
    jsonParser.teardown();
  }

  @Test
  public void testNestedReads() throws Exception
  {
    int count = 4;
    createReaderInput(count);
    jsonParser.setFieldMappingString(nestedFieldInfoMap);
    jsonParser.setup(testMeta.context);
    jsonParser.output.setup(testMeta.portContext);

    jsonParser.beginWindow(0);
    ListIterator<String> itr = recordList.listIterator();
    while (itr.hasNext()) {
      jsonParser.in.process(itr.next().getBytes());
    }
    jsonParser.endWindow();
    Person obj = (Person)outputSink.collectedTuples.get(0);
    Assert.assertEquals("Number of tuples", count, outputSink.collectedTuples.size());
    Assert.assertEquals("Name is", "name-4", obj.getName());
    jsonParser.teardown();
  }

  @Test
  public void testReadsWithReflection() throws Exception
  {
    int count = 6;
    createReaderInput(count);
    jsonParser.setFieldMappingString(null);
    jsonParser.setup(testMeta.context);
    jsonParser.output.setup(testMeta.portContext);

    jsonParser.beginWindow(0);
    ListIterator<String> itr = recordList.listIterator();
    while (itr.hasNext()) {
      jsonParser.in.process(itr.next().getBytes());
    }
    jsonParser.endWindow();
    Person obj = (Person)outputSink.collectedTuples.get(0);
    Assert.assertEquals("Number of tuples", count, outputSink.collectedTuples.size());
    Assert.assertEquals("Name is", "name-6", obj.getName());
    jsonParser.teardown();
  }

  @Test
  public void testInvalidKeyMapping() throws Exception
  {
    int count = 6;
    createReaderInput(count);
    jsonParser.setFieldMappingString(invalidFieldInfoMap);
    jsonParser.setup(testMeta.context);
    jsonParser.output.setup(testMeta.portContext);

    jsonParser.beginWindow(0);
    ListIterator<String> itr = recordList.listIterator();
    while (itr.hasNext()) {
      jsonParser.in.process(itr.next().getBytes());
    }
    jsonParser.endWindow();
    Person obj = (Person)outputSink.collectedTuples.get(0);
    Assert.assertEquals("Number of tuples", count, outputSink.collectedTuples.size());
    Assert.assertEquals("Id is", null, obj.getId());
    Assert.assertEquals("Name is", "name-6", obj.getName());
    jsonParser.teardown();
  }

  private void createReaderInput(int count)
  {
    String address = "\"address\":{" + "\"streetAddress\": \"21 2nd Street\"," + "\"city\": \"New York\","
        + "\"state\": \"NY\"," + "\"postalCode\": \"10021\"}";
    recordList = Lists.newArrayList();
    while (count > 0) {
      StringBuilder sb = new StringBuilder();
      sb.append("{").append("\"id\"").append(":").append(count).append(",");
      sb.append("\"name\":").append("\"").append("name-" + count).append("\"").append(",");
      sb.append("\"Elective-0\":").append("\"").append("elective-" + count * 1).append("\"").append(",");
      sb.append("\"Elective-1\":").append("\"").append("elective-" + count * 2).append("\"").append(",");
      sb.append("\"Elective-2\":").append("\"").append("elective-" + count * 3).append("\"").append(",");
      sb.append("\"Elective-3\":").append("\"").append("elective-" + count * 4).append("\"").append(",");
      sb.append("\"gpa\":").append(count * 2.0).append(",");
      sb.append(address).append("}");
      count--;
      recordList.add(sb.toString());
    }
  }

  private void writeJsonInputFile(File file)
  {
    try {
      // if file doesnt exists, then create it
      if (!file.exists()) {
        file.createNewFile();
      }
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      ListIterator<String> itr = recordList.listIterator();
      while (itr.hasNext()) {
        bw.write(itr.next().toString());
      }
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testApplicationWithPojoConversion() throws IOException, Exception
  {
    try {
      FileContext.getLocalFSFileContext().delete(new Path(new File(testMeta.dir).getAbsolutePath()), true);
      int cnt = 7;
      createReaderInput(cnt);
      writeJsonInputFile(new File(FILENAME));
      FileInputOperator fileInput = new FileInputOperator();
      fileInput.setDirectory(testMeta.dir);
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      JsonStreamingParserApp streamingParserApp = new JsonStreamingParserApp();
      streamingParserApp.setParser(jsonParser);
      streamingParserApp.setFileInput(fileInput);
      lma.prepareDAG(streamingParserApp, conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000);// runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  public static class FileInputOperator extends AbstractFileInputOperator<String>
  {
    public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();

    protected transient BufferedReader br;

    @Override
    protected InputStream openFile(Path path) throws IOException
    {
      InputStream is = super.openFile(path);
      br = new BufferedReader(new InputStreamReader(is));
      return is;
    }

    @Override
    protected void closeFile(InputStream is) throws IOException
    {
      super.closeFile(is);
      br.close();
      br = null;
    }

    @Override
    protected String readEntity() throws IOException
    {
      return br.readLine();
    }

    @Override
    protected void emit(String tuple)
    {
      output.emit(tuple.getBytes());
    }
  }

  public static class JsonStreamingParserApp implements StreamingApplication
  {

    StreamingJsonParser parser;
    FileInputOperator fileInput;

    public FileInputOperator getFileInput()
    {
      return fileInput;
    }

    public void setFileInput(FileInputOperator fileInput)
    {
      this.fileInput = fileInput;
    }

    public StreamingJsonParser getParser()
    {
      return parser;
    }

    public void setParser(StreamingJsonParser parser)
    {
      this.parser = parser;
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      FileInputOperator fileInput = dag.addOperator("fileInput", getFileInput());
      StreamingJsonParser parser = dag.addOperator("parser", getParser());
      dag.getMeta(parser).getMeta(parser.output).getAttributes().put(Context.PortContext.TUPLE_CLASS, Person.class);
      ConsoleOutputOperator consoleOutput = dag.addOperator("output", new ConsoleOutputOperator());
      dag.addStream("Input", fileInput.output, parser.in).setLocality(Locality.CONTAINER_LOCAL);
      dag.addStream("pojo", parser.output, consoleOutput.input).setLocality(Locality.CONTAINER_LOCAL);
    }

  }

  public static class Person
  {
    private Integer id;
    private String name;
    private Double gpa;
    private String streetAddress;
    private String city;
    private String postalCode;
    private String state;

    public Integer getId()
    {
      return id;
    }

    public void setId(Integer id)
    {
      this.id = id;
    }

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public Double getGpa()
    {
      return gpa;
    }

    public void setGpa(Double gpa)
    {
      this.gpa = gpa;
    }

    public String getStreetAddress()
    {
      return streetAddress;
    }

    public void setStreetAddress(String streetAddress)
    {
      this.streetAddress = streetAddress;
    }

    public String getCity()
    {
      return city;
    }

    public void setCity(String city)
    {
      this.city = city;
    }

    public String getPostalCode()
    {
      return postalCode;
    }

    public void setPostalCode(String postalCode)
    {
      this.postalCode = postalCode;
    }

    public String getState()
    {
      return state;
    }

    public void setState(String state)
    {
      this.state = state;
    }
  }
}
