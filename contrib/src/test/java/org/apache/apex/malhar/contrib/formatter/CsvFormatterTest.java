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
package org.apache.apex.malhar.contrib.formatter;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.contrib.parser.CsvPOJOParserTest.Ad;
import org.apache.apex.malhar.lib.appdata.schemas.SchemaUtils;
import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;

public class CsvFormatterTest
{

  private static final String filename = "schema.json";
  CsvFormatter operator;
  CollectorTestSink<Object> validDataSink;
  CollectorTestSink<String> invalidDataSink;

  @Rule
  public Watcher watcher = new Watcher();

  public class Watcher extends TestWatcher
  {

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      operator = new CsvFormatter();
      operator.setClazz(Ad.class);
      operator.setSchema(SchemaUtils.jarResourceFileToString(filename));
      validDataSink = new CollectorTestSink<Object>();
      invalidDataSink = new CollectorTestSink<String>();
      TestUtils.setSink(operator.out, validDataSink);
      TestUtils.setSink(operator.err, invalidDataSink);
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      operator.teardown();
    }

  }

  @Test
  public void testPojoReaderToCsv()
  {
    operator.setup(null);
    Ad ad = new Ad();
    ad.setCampaignId(9823);
    ad.setAdId(1234);
    ad.setAdName("ad");
    ad.setBidPrice(1.2);
    ad.setStartDate(
        new DateTime().withDate(2015, 1, 1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).toDate());
    ad.setEndDate(new DateTime().withDate(2016, 1, 1).toDate());
    ad.setSecurityCode(12345678);
    ad.setParentCampaign("CAMP_AD");
    ad.setActive(true);
    ad.setWeatherTargeted('y');
    ad.setValid("valid");
    operator.in.process(ad);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String csvOp = (String)validDataSink.collectedTuples.get(0);
    Assert.assertNotNull(csvOp);
    Assert.assertEquals("1234,9823,ad,1.2,2015-01-01 00:00:00,01/01/2016,12345678,true,false,CAMP_AD,y,valid\r\n",
        csvOp);
    Assert.assertEquals(1, operator.getIncomingTuplesCount());
    Assert.assertEquals(0, operator.getErrorTupleCount());
    Assert.assertEquals(1, operator.getEmittedObjectCount());
  }

  @Test
  public void testPojoReaderToCsvNullInput()
  {
    operator.setup(null);
    operator.in.process(null);
    Assert.assertEquals(0, validDataSink.collectedTuples.size());
    Assert.assertEquals(1, invalidDataSink.collectedTuples.size());
    Assert.assertEquals(1, operator.getIncomingTuplesCount());
    Assert.assertEquals(1, operator.getErrorTupleCount());
    Assert.assertEquals(0, operator.getEmittedObjectCount());

  }

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new CsvParserApplication(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(5000);// runs for 5 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  public static class CsvParserApplication implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      PojoEmitter input = dag.addOperator("data", new PojoEmitter());
      CsvFormatter formatter = dag.addOperator("formatter", new CsvFormatter());
      dag.getMeta(formatter).getMeta(formatter.in).getAttributes().put(Context.PortContext.TUPLE_CLASS, Ad.class);
      formatter.setSchema(SchemaUtils.jarResourceFileToString("schema.json"));
      ConsoleOutputOperator output = dag.addOperator("output", new ConsoleOutputOperator());
      ConsoleOutputOperator error = dag.addOperator("error", new ConsoleOutputOperator());
      output.setDebug(true);
      dag.addStream("input", input.output, formatter.in);
      dag.addStream("output", formatter.out, output.input);
      dag.addStream("err", formatter.err, error.input);
    }
  }

  public static class PojoEmitter extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<Object>();

    @Override
    public void emitTuples()
    {
      Ad ad = new Ad();
      ad.setCampaignId(9823);
      ad.setAdId(1234);
      ad.setAdName("ad");
      ad.setBidPrice(1.2);
      ad.setStartDate(
          new DateTime().withDate(2015, 1, 1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).toDate());
      ad.setEndDate(new DateTime().withDate(2016, 1, 1).toDate());
      ad.setSecurityCode(12345678);
      ad.setParentCampaign("CAMP_AD");
      ad.setActive(true);
      ad.setWeatherTargeted('y');
      ad.setValid("valid");
      output.emit(ad);
    }
  }

}
