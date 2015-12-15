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
package com.datatorrent.lib.parser;

import java.util.Date;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class XmlParserTest
{
  XmlParser operator;
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
      operator = new XmlParser();
      operator.setClazz(EmployeeBean.class);
      operator.setDateFormats("yyyy-MM-dd"); //setting default date pattern
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
  public void testXmlToPojoWithoutAlias()
  {
    String tuple = "<com.datatorrent.contrib.schema.parser.XmlParserTest_-EmployeeBean>" + "<name>john</name>"
        + "<dept>cs</dept>" + "<eid>1</eid>" + "<dateOfJoining>2015-01-01</dateOfJoining>"
        + "</com.datatorrent.contrib.schema.parser.XmlParserTest_-EmployeeBean>";

    operator.setup(null);
    operator.activate(null);
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
    Assert.assertEquals(2015, new DateTime(pojo.getDateOfJoining()).getYear());
    Assert.assertEquals(1, new DateTime(pojo.getDateOfJoining()).getMonthOfYear());
    Assert.assertEquals(1, new DateTime(pojo.getDateOfJoining()).getDayOfMonth());
  }

  @Test
  public void testXmlToPojoWithAliasDateFormat()
  {
    String tuple = "<EmployeeBean>" + "<name>john</name>" + "<dept>cs</dept>" + "<eid>1</eid>"
        + "<dateOfJoining>2015-JAN-01</dateOfJoining>" + "</EmployeeBean>";

    operator.setAlias("EmployeeBean");
    operator.setDateFormats("yyyy-MM-dd,yyyy-MMM-dd");
    operator.setup(null);
    operator.activate(null);
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
    Assert.assertEquals(2015, new DateTime(pojo.getDateOfJoining()).getYear());
    Assert.assertEquals(1, new DateTime(pojo.getDateOfJoining()).getMonthOfYear());
    Assert.assertEquals(1, new DateTime(pojo.getDateOfJoining()).getDayOfMonth());
  }

  @Test
  public void testXmlToPojoWithAlias()
  {
    String tuple = "<EmployeeBean>" + "<name>john</name>" + "<dept>cs</dept>" + "<eid>1</eid>"
        + "<dateOfJoining>2015-01-01</dateOfJoining>" + "</EmployeeBean>";

    operator.setAlias("EmployeeBean");
    operator.setup(null);
    operator.activate(null);
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
    Assert.assertEquals(2015, new DateTime(pojo.getDateOfJoining()).getYear());
    Assert.assertEquals(1, new DateTime(pojo.getDateOfJoining()).getMonthOfYear());
    Assert.assertEquals(1, new DateTime(pojo.getDateOfJoining()).getDayOfMonth());
  }

  @Test
  public void testXmlToPojoIncorrectXML()
  {
    String tuple = "<EmployeeBean>"
        + "<firstname>john</firstname>" //incorrect field name
        + "<dept>cs</dept>" + "<eid>1</eid>" + "<dateOfJoining>2015-01-01 00:00:00.00 IST</dateOfJoining>"
        + "</EmployeeBean>";

    operator.setAlias("EmployeeBean");
    operator.setup(null);
    operator.activate(null);
    operator.in.process(tuple);
    Assert.assertEquals(0, validDataSink.collectedTuples.size());
    Assert.assertEquals(1, invalidDataSink.collectedTuples.size());
    Assert.assertEquals(tuple, invalidDataSink.collectedTuples.get(0));
  }

  @Test
  public void testXmlToPojoWithoutAliasHeirarchical()
  {
    String tuple = "<com.datatorrent.contrib.schema.parser.XmlParserTest_-EmployeeBean>" + "<name>john</name>"
        + "<dept>cs</dept>" + "<eid>1</eid>" + "<dateOfJoining>2015-01-01</dateOfJoining>" + "<address>"
        + "<city>new york</city>" + "<country>US</country>" + "</address>"
        + "</com.datatorrent.contrib.schema.parser.XmlParserTest_-EmployeeBean>";

    operator.setup(null);
    operator.activate(null);
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
    Assert.assertEquals(Address.class, pojo.getAddress().getClass());
    Assert.assertEquals("new york", pojo.getAddress().getCity());
    Assert.assertEquals("US", pojo.getAddress().getCountry());
    Assert.assertEquals(2015, new DateTime(pojo.getDateOfJoining()).getYear());
    Assert.assertEquals(1, new DateTime(pojo.getDateOfJoining()).getMonthOfYear());
    Assert.assertEquals(1, new DateTime(pojo.getDateOfJoining()).getDayOfMonth());
  }

  public static class EmployeeBean
  {

    private String name;
    private String dept;
    private int eid;
    private Date dateOfJoining;
    private Address address;

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

    public Address getAddress()
    {
      return address;
    }

    public void setAddress(Address address)
    {
      this.address = address;
    }
  }

  public static class Address
  {

    private String city;
    private String country;

    public String getCity()
    {
      return city;
    }

    public void setCity(String city)
    {
      this.city = city;
    }

    public String getCountry()
    {
      return country;
    }

    public void setCountry(String country)
    {
      this.country = country;
    }
  }

}
