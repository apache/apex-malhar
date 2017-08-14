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
package org.apache.apex.malhar.lib.formatter;

import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.XMLFormatter;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.TestUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class XmlFormatterTest
{

  XmlFormatter operator;
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
      operator = new XmlFormatter();
      operator.setClazz(EmployeeBean.class);
      operator.setDateFormat("yyyy-MM-dd");
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
    XMLFormatter tba1 = kryo.readObject(input, XMLFormatter.class);
    Assert.assertNotNull("XML parser not null", tba1);
  }

  @Test
  public void testPojoToXmlWithoutAlias()
  {
    EmployeeBean e = new EmployeeBean();
    e.setName("john");
    e.setEid(1);
    e.setDept("cs");
    e.setDateOfJoining(new DateTime().withYear(2015).withMonthOfYear(1).withDayOfYear(1).toDate());

    operator.setup(null);
    operator.in.process(e);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expected = "<EmployeeBean>" + "<name>john</name>"
        + "<dept>cs</dept>" + "<eid>1</eid>" + "<dateOfJoining>2015-01-01</dateOfJoining>"
        + "</EmployeeBean>";
    Assert.assertEquals(expected, validDataSink.collectedTuples.get(0));
  }

  @Test
  public void testXmlToPojoWithAlias()
  {
    EmployeeBean e = new EmployeeBean();
    e.setName("john");
    e.setEid(1);
    e.setDept("cs");
    e.setDateOfJoining(new DateTime().withYear(2015).withMonthOfYear(1).withDayOfYear(1).toDate());

    operator.setAlias("EmployeeBean");
    operator.setup(null);
    operator.in.process(e);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expected = "<EmployeeBean>" + "<name>john</name>" + "<dept>cs</dept>" + "<eid>1</eid>"
        + "<dateOfJoining>2015-01-01</dateOfJoining>" + "</EmployeeBean>";
    Assert.assertEquals(expected, validDataSink.collectedTuples.get(0));
  }

  @Test
  public void testXmlToPojoWithPrettyPrint()
  {
    EmployeeBean e = new EmployeeBean();
    e.setName("john");
    e.setEid(1);
    e.setDept("cs");
    e.setDateOfJoining(new DateTime().withYear(2015).withMonthOfYear(1).withDayOfYear(1).toDate());

    operator.setAlias("EmployeeBean");
    operator.setPrettyPrint(true);
    operator.setup(null);
    operator.in.process(e);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expected = "<EmployeeBean>\n" + "    <name>john</name>\n" + "    <dept>cs</dept>\n" + "    <eid>1</eid>\n"
        + "    <dateOfJoining>2015-01-01</dateOfJoining>\n" + "</EmployeeBean>";
    Assert.assertEquals(expected, validDataSink.collectedTuples.get(0));
  }

  @Test
  public void testPojoToXmlWithoutAliasHeirarchical()
  {
    EmployeeBean e = new EmployeeBean();
    e.setName("john");
    e.setEid(1);
    e.setDept("cs");
    e.setDateOfJoining(new DateTime().withYear(2015).withMonthOfYear(1).withDayOfYear(1).toDate());
    Address address = new Address();
    address.setCity("new york");
    address.setCountry("US");
    e.setAddress(address);

    operator.setup(null);
    operator.in.process(e);
    LOG.debug("{}", validDataSink.collectedTuples.get(0));
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expected = "<EmployeeBean>" + "<name>john</name>"
        + "<dept>cs</dept>" + "<eid>1</eid>" + "<dateOfJoining>2015-01-01</dateOfJoining>" + "<address>"
        + "<city>new york</city>" + "<country>US</country>" + "</address>"
        + "</EmployeeBean>";
    Assert.assertEquals(expected, validDataSink.collectedTuples.get(0));
  }

  public static class DateAdapter extends XmlAdapter<String, Date>
  {

    private String dateFormatString = "yyyy-MM-dd";
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

  @XmlType(propOrder = {"name", "dept", "eid", "dateOfJoining", "address"})
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

    @XmlJavaTypeAdapter(DateAdapter.class)
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

  private static final Logger LOG = LoggerFactory.getLogger(XmlFormatterTest.class);

}
