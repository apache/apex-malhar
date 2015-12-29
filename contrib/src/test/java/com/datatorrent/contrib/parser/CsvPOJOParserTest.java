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
package com.datatorrent.contrib.parser;

import java.util.Date;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class CsvPOJOParserTest
{

  CsvParser operator;
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
      operator = new CsvParser();
      operator.setClazz(EmployeeBean.class);
      operator.setFieldInfo("name:string,dept:string,eid:integer,dateOfJoining:date");
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
  public void testCsvToPojoWriterDefault()
  {
    operator.setup(null);
    String tuple = "john,cs,1,01/01/2015";
    operator.in.process(tuple);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    Object obj = validDataSink.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(EmployeeBean.class, obj.getClass());
    EmployeeBean pojo = (EmployeeBean)obj;
    Assert.assertEquals("john", pojo.getName());
    Assert.assertEquals("cs", pojo.getDept());
    Assert.assertEquals(1, pojo.getEid());
    Assert.assertEquals(new DateTime().withDate(2015, 1, 1).withMillisOfDay(0).withTimeAtStartOfDay(), new DateTime(
        pojo.getDateOfJoining()));
  }

  @Test
  public void testCsvToPojoWriterDateFormat()
  {
    operator.setFieldInfo("name:string,dept:string,eid:integer,dateOfJoining:date|dd-MMM-yyyy");
    operator.setup(null);
    String tuple = "john,cs,1,01-JAN-2015";
    operator.in.process(tuple);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    Object obj = validDataSink.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(EmployeeBean.class, obj.getClass());
    EmployeeBean pojo = (EmployeeBean)obj;
    Assert.assertEquals("john", pojo.getName());
    Assert.assertEquals("cs", pojo.getDept());
    Assert.assertEquals(1, pojo.getEid());
    Assert.assertEquals(new DateTime().withDate(2015, 1, 1).withMillisOfDay(0).withTimeAtStartOfDay(), new DateTime(
        pojo.getDateOfJoining()));
  }

  @Test
  public void testCsvToPojoWriterDateFormatMultiple()
  {
    operator.setFieldInfo("name:string,dept:string,eid:integer,dateOfJoining:date|dd-MMM-yyyy,dateOfBirth:date");
    operator.setup(null);
    String tuple = "john,cs,1,01-JAN-2015,01/01/2015";
    operator.in.process(tuple);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    Object obj = validDataSink.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(EmployeeBean.class, obj.getClass());
    EmployeeBean pojo = (EmployeeBean)obj;
    Assert.assertEquals("john", pojo.getName());
    Assert.assertEquals("cs", pojo.getDept());
    Assert.assertEquals(1, pojo.getEid());
    Assert.assertEquals(new DateTime().withDate(2015, 1, 1).withMillisOfDay(0).withTimeAtStartOfDay(), new DateTime(
        pojo.getDateOfJoining()));
    Assert.assertEquals(new DateTime().withDate(2015, 1, 1).withMillisOfDay(0).withTimeAtStartOfDay(), new DateTime(
        pojo.getDateOfBirth()));
  }

  public static class EmployeeBean
  {

    private String name;
    private String dept;
    private int eid;
    private Date dateOfJoining;
    private Date dateOfBirth;

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public String getDept()
    {
      return dept;
    }

    public void setDept(String dept)
    {
      this.dept = dept;
    }

    public int getEid()
    {
      return eid;
    }

    public void setEid(int eid)
    {
      this.eid = eid;
    }

    public Date getDateOfJoining()
    {
      return dateOfJoining;
    }

    public void setDateOfJoining(Date dateOfJoining)
    {
      this.dateOfJoining = dateOfJoining;
    }

    public Date getDateOfBirth()
    {
      return dateOfBirth;
    }

    public void setDateOfBirth(Date dateOfBirth)
    {
      this.dateOfBirth = dateOfBirth;
    }
  }

}
