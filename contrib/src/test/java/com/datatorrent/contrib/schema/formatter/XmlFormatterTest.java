package com.datatorrent.contrib.schema.formatter;

import java.util.Date;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.contrib.schema.formatter.XmlFormatter;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

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
  public void testPojoToXmlWithoutAlias()
  {
    EmployeeBean e = new EmployeeBean();
    e.setName("john");
    e.setEid(1);
    e.setDept("cs");
    e.setDateOfJoining(new DateTime().withYear(2015).withMonthOfYear(1).withDayOfYear(1).toDate());

    operator.setup(null);
    operator.activate(null);
    operator.in.process(e);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expected = "<com.datatorrent.contrib.schema.formatter.XmlFormatterTest_-EmployeeBean>" + "<name>john</name>"
        + "<dept>cs</dept>" + "<eid>1</eid>" + "<dateOfJoining>2015-01-01</dateOfJoining>"
        + "</com.datatorrent.contrib.schema.formatter.XmlFormatterTest_-EmployeeBean>";
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
    operator.activate(null);
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
    operator.activate(null);
    operator.in.process(e);
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expected = "<EmployeeBean>\n" + "  <name>john</name>\n" + "  <dept>cs</dept>\n" + "  <eid>1</eid>\n"
        + "  <dateOfJoining>2015-01-01</dateOfJoining>\n" + "</EmployeeBean>";
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
    operator.activate(null);
    operator.in.process(e);
    System.out.println(validDataSink.collectedTuples.get(0));
    Assert.assertEquals(1, validDataSink.collectedTuples.size());
    Assert.assertEquals(0, invalidDataSink.collectedTuples.size());
    String expected = "<com.datatorrent.contrib.schema.formatter.XmlFormatterTest_-EmployeeBean>" + "<name>john</name>"
        + "<dept>cs</dept>" + "<eid>1</eid>" + "<dateOfJoining>2015-01-01</dateOfJoining>" + "<address>"
        + "<city>new york</city>" + "<country>US</country>" + "</address>"
        + "</com.datatorrent.contrib.schema.formatter.XmlFormatterTest_-EmployeeBean>";
    Assert.assertEquals(expected, validDataSink.collectedTuples.get(0));
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
