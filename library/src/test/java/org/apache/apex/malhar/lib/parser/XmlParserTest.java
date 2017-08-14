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
package org.apache.apex.malhar.lib.parser;

import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

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
  public void testOperatorSerialization()
  {
    Kryo kryo = new Kryo();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeObject(output, this.operator);
    output.close();
    Input input = new Input(baos.toByteArray());
    XmlParser tba1 = kryo.readObject(input, XmlParser.class);
    Assert.assertNotNull("XML parser not null", tba1);
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
        + "<dateOfJoiningYYYYMMDDFormat>2015-JAN-01</dateOfJoiningYYYYMMDDFormat>" + "</EmployeeBean>";

    operator.setClazz(EmployeeBeanOverride.class);
    operator.setup(null);
    operator.activate(null);
    operator.in.process(tuple);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    Object obj = validDataSink.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(EmployeeBeanOverride.class, obj.getClass());
    EmployeeBeanOverride pojo = (EmployeeBeanOverride)obj;
    Assert.assertEquals("john", pojo.getName());
    Assert.assertEquals("cs", pojo.getDept());
    Assert.assertEquals(1, pojo.getEid());
    Assert.assertEquals(2015, new DateTime(pojo.getDateOfJoining()).getYear());
    Assert.assertEquals(1, new DateTime(pojo.getDateOfJoining()).getMonthOfYear());
    Assert.assertEquals(1, new DateTime(pojo.getDateOfJoining()).getDayOfMonth());
  }

  @Test
  public void testXSDValidation()
  {
    String xsdFile = "src/test/resources/employeeBean.xsd";
    operator.setSchemaFile(xsdFile);
    // Check without address field xsd validation fails
    String tuple = "<EmployeeBean>" + "<name>john</name>" + "<dept>cs</dept>" + "<eid>1</eid>"
        + "<dateOfJoining>2015-01-01</dateOfJoining>" + "</EmployeeBean>";
    operator.setup(null);
    operator.activate(null);
    operator.in.process(tuple);
    Assert.assertEquals(0, validDataSink.collectedTuples.size());
    Assert.assertEquals(1, invalidDataSink.collectedTuples.size());
    Assert.assertEquals(tuple, invalidDataSink.collectedTuples.get(0));

    // Check extra fields present in xml, which are not part of xsd
    tuple = "<EmployeeBean>" + "<name>john</name>"
        + "<firstname>john</firstname>" //incorrect field name, xsd validation would fail
        + "<dept>cs</dept>" + "<eid>1</eid>" + "<dateOfJoining>2015-01-01</dateOfJoining>" + "<address>"
        + "<city>new york</city>" + "<country>US</country>" + "</address>" + "</EmployeeBean>";

    validDataSink.collectedTuples.clear();
    invalidDataSink.collectedTuples.clear();
    operator.in.process(tuple);
    Assert.assertEquals(0, validDataSink.collectedTuples.size());
    Assert.assertEquals(1, invalidDataSink.collectedTuples.size());
    Assert.assertEquals(tuple, invalidDataSink.collectedTuples.get(0));

    // Check with all fields in xsd, POJO output should be received

    tuple = "<EmployeeBean>" + "<name>john</name>" + "<dept>cs</dept>" + "<eid>1</eid>"
        + "<dateOfJoining>2015-01-01</dateOfJoining>" + "<address>" + "<city>new york</city>" + "<country>US</country>"
        + "</address>" + "</EmployeeBean>";

    validDataSink.collectedTuples.clear();
    invalidDataSink.collectedTuples.clear();
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
    Assert.assertEquals(Address.class, pojo.getAddress().getClass());
    Assert.assertEquals("new york", pojo.getAddress().getCity());
    Assert.assertEquals("US", pojo.getAddress().getCountry());
  }

  @Test
  public void testXmlToPojoWithAlias()
  {
    String tuple = "<EmployeeBean>" + "<name>john</name>" + "<dept>cs</dept>" + "<eid>1</eid>"
        + "<dateOfJoining>2015-01-01</dateOfJoining>" + "</EmployeeBean>";

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
    String tuple = "<EmployeeBean>" + "<firstname>john</firstname>" //incorrect field name is ignored by JAXB
        + "<dept>cs</dept>" + "<eid>1</eid>" + "<dateOfJoining>2015-01-01 00:00:00.00 IST</dateOfJoining>";
    // + "</EmployeeBean>"; // Incorrect XML format

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

  public static class DateAdapter extends XmlAdapter<String, Date>
  {

    private String dateFormatString = "yyyy-MMM-dd";
    private SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString);

    @Override
    public String marshal(Date v) throws Exception
    {
      return dateFormat.format(v);
    }

    @Override
    public Date unmarshal(String v) throws Exception
    {
      return dateFormat.parse(v);
    }

    public String getDateFormatString()
    {
      return dateFormatString;
    }

    public void setDateFormatString(String dateFormatString)
    {
      this.dateFormatString = dateFormatString;
    }

  }

  public static class EmployeeBeanOverride extends EmployeeBean
  {
    @XmlJavaTypeAdapter(DateAdapter.class)
    public void setDateOfJoiningYYYYMMDDFormat(Date dateOfJoining)
    {
      this.dateOfJoining = dateOfJoining;
    }
  }

  public static class EmployeeBean
  {

    @Override
    public String toString()
    {
      return "EmployeeBean [name=" + name + ", dept=" + dept + ", eid=" + eid + ", dateOfJoining=" + dateOfJoining
          + ", address=" + address + "]";
    }

    private String name;
    private String dept;
    private int eid;
    protected Date dateOfJoining;
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

    @Override
    public String toString()
    {
      return "Address [city=" + city + ", country=" + country + "]";
    }

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
