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
package com.datatorrent.contrib.schema.formatter;

import java.util.Date;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.contrib.schema.formatter.CsvFormatter;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class CsvFormatterTest
{

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
      operator.setFieldInfo("name:string,dept:string,eid:integer,dateOfJoining:date");
      operator.setLineDelimiter("\r\n");
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
    EmployeeBean emp = new EmployeeBean();
    emp.setName("john");
    emp.setDept("cs");
    emp.setEid(1);
    emp.setDateOfJoining(new DateTime().withDate(2015, 1, 1).toDate());
    operator.in.process(emp);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String csvOp = (String)validDataSink.collectedTuples.get(0);
    Assert.assertNotNull(csvOp);
    Assert.assertEquals("john,cs,1,01/01/2015" + operator.getLineDelimiter(), csvOp);
  }

  @Test
  public void testPojoReaderToCsvMultipleDate()
  {
    operator.setFieldInfo("name:string,dept:string,eid:integer,dateOfJoining:date,dateOfBirth:date|dd-MMM-yyyy");
    operator.setup(null);
    EmployeeBean emp = new EmployeeBean();
    emp.setName("john");
    emp.setDept("cs");
    emp.setEid(1);
    emp.setDateOfJoining(new DateTime().withDate(2015, 1, 1).toDate());
    emp.setDateOfBirth(new DateTime().withDate(2015, 1, 1).toDate());
    operator.in.process(emp);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String csvOp = (String)validDataSink.collectedTuples.get(0);
    Assert.assertNotNull(csvOp);
    Assert.assertEquals("john,cs,1,01/01/2015,01-Jan-2015" + operator.getLineDelimiter(), csvOp);
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
