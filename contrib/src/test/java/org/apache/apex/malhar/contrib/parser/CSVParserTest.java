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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvMapReader;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.apex.malhar.contrib.parser.AbstractCsvParser.Field;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.ReusableStringReader;
import org.apache.apex.malhar.lib.util.TestUtils.TestInfo;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.netlet.util.DTThrowable;

public class CSVParserTest
{
  private static final String filename = "test.txt";
  @Rule
  public TestInfo testMeta = new TestWatcher();

  public static class TestWatcher extends TestInfo
  {
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      new File(getDir()).mkdir();
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      FileUtils.deleteQuietly(new File(getDir()));
    }

  }

  @Test
  public void TestParserWithHeader()
  {
    CsvToMapParser parser = new CsvToMapParser();
    parser.setFieldDelimiter(',');
    parser.setLineDelimiter("\n");
    parser.setHasHeader(true);
    ArrayList<CsvToMapParser.Field> fields = new ArrayList<CsvToMapParser.Field>();
    Field field1 = new Field();
    field1.setName("Eid");
    field1.setType("INTEGER");
    fields.add(field1);
    Field field2 = new Field();
    field2.setName("Name");
    field2.setType("STRING");
    fields.add(field2);
    Field field3 = new Field();
    field3.setName("Salary");
    field3.setType("LONG");
    fields.add(field3);
    parser.setFields(fields);
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    parser.output.setSink(sink);
    parser.setup(null);
    String input = "Eid,Name,Salary\n123,xyz,567777\n321,abc,7777000\n456,pqr,5454545454";
    parser.input.process(input.getBytes());
    parser.teardown();
    Assert.assertEquals("Tuples read should be same ", 6, sink.collectedTuples.size());
    Assert.assertEquals("Eid", sink.collectedTuples.get(0));
    Assert.assertEquals("Name", sink.collectedTuples.get(1));
    Assert.assertEquals("Salary", sink.collectedTuples.get(2));
    Assert.assertEquals("{Name=xyz, Salary=567777, Eid=123}", sink.collectedTuples.get(3).toString());
    Assert.assertEquals("{Name=abc, Salary=7777000, Eid=321}", sink.collectedTuples.get(4).toString());
    Assert.assertEquals("{Name=pqr, Salary=5454545454, Eid=456}", sink.collectedTuples.get(5).toString());
    sink.clear();

  }

  @Test
  public void TestParserWithoutHeader()
  {
    CsvToMapParser parser = new CsvToMapParser();
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    parser.output.setSink(sink);
    parser.setFieldDelimiter(',');
    parser.setLineDelimiter("\n");
    ArrayList<CsvToMapParser.Field> fields = new ArrayList<CsvToMapParser.Field>();
    Field field1 = new Field();
    field1.setName("Eid");
    field1.setType("INTEGER");
    fields.add(field1);
    Field field2 = new Field();
    field2.setName("Name");
    field2.setType("STRING");
    fields.add(field2);
    Field field3 = new Field();
    field3.setName("Salary");
    field3.setType("LONG");
    fields.add(field3);
    parser.setFields(fields);
    parser.setHasHeader(false);
    parser.setup(null);
    String input = "123,xyz,567777\n321,abc,7777000\n456,pqr,5454545454";
    parser.input.process(input.getBytes());
    parser.teardown();

    Assert.assertEquals("Tuples read should be same ", 3, sink.collectedTuples.size());
    Assert.assertEquals("{Name=xyz, Salary=567777, Eid=123}", sink.collectedTuples.get(0).toString());
    Assert.assertEquals("{Name=abc, Salary=7777000, Eid=321}", sink.collectedTuples.get(1).toString());
    Assert.assertEquals("{Name=pqr, Salary=5454545454, Eid=456}", sink.collectedTuples.get(2).toString());
    sink.clear();
  }

  @Test
  public void TestParserWithFileInput()
  {
    createFieldMappingFile();
    CsvToMapParser parser = new CsvToMapParser();
    CollectorTestSink<Object> sink = new CollectorTestSink<Object>();
    parser.output.setSink(sink);
    parser.setFieldDelimiter(',');
    parser.setLineDelimiter("\n");
    parser.setFieldmappingFile(testMeta.getDir() + "/" + filename);
    parser.setHasHeader(false);
    parser.setup(null);
    String input = "123,xyz,567777\n321,abc,7777000\n456,pqr,5454545454";
    parser.input.process(input.getBytes());
    parser.teardown();

    Assert.assertEquals("Tuples read should be same ", 3, sink.collectedTuples.size());
    Assert.assertEquals("{Name=xyz, Salary=567777, Eid=123}", sink.collectedTuples.get(0).toString());
    Assert.assertEquals("{Name=abc, Salary=7777000, Eid=321}", sink.collectedTuples.get(1).toString());
    Assert.assertEquals("{Name=pqr, Salary=5454545454, Eid=456}", sink.collectedTuples.get(2).toString());
    sink.clear();
  }

  public void createFieldMappingFile()
  {
    FileSystem hdfs = null;
    //Creating a file in HDFS
    Path newFilePath = new Path(testMeta.getDir() + "/" + filename);
    try {
      hdfs = FileSystem.get(new Configuration());
      hdfs.createNewFile(newFilePath);
    } catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    //Writing data to a HDFS file
    StringBuilder sb = new StringBuilder();
    sb.append("Eid");
    sb.append(":");
    sb.append("INTEGER");
    sb.append("\n");
    sb.append("Name");
    sb.append(":");
    sb.append("STRING");
    sb.append("\n");
    sb.append("Salary");
    sb.append(":");
    sb.append("LONG");
    sb.append("\n");
    byte[] byt = sb.toString().getBytes();
    try {
      FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
      fsOutStream.write(byt);
      fsOutStream.close();
    } catch (IOException ex) {
      DTThrowable.rethrow(ex);

    }
    logger.debug("Written data to HDFS file.");
  }

  private class CsvToMapParser extends AbstractCsvParser<Map<String, Object>>
  {
    protected transient ICsvMapReader csvReader = null;

    /**
     * This method creates an instance of csvMapReader.
     *
     * @param reader
     * @param preference
     * @return CSV Map Reader
     */
    @Override
    protected ICsvMapReader getReader(ReusableStringReader reader, CsvPreference preference)
    {
      csvReader = new CsvMapReader(reader, preference);
      return csvReader;
    }

    /**
     * This method reads input stream data values into a map.
     *
     * @return Map containing key as field name given by user and value of the field.
     */
    @Override
    protected Map<String, Object> readData(String[] properties, CellProcessor[] processors)
    {
      Map<String, Object> fieldValueMapping = null;
      try {
        fieldValueMapping = csvReader.read(properties, processors);
      } catch (IOException ex) {
        DTThrowable.rethrow(ex);
      }
      return fieldValueMapping;

    }

  }

  private static final Logger logger = LoggerFactory.getLogger(CSVParserTest.class);
}
