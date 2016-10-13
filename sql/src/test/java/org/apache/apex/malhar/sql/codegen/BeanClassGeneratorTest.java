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
package org.apache.apex.malhar.sql.codegen;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;

import org.codehaus.jettison.json.JSONException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.utils.ClassLoaderUtils;
import org.apache.apex.malhar.sql.schema.TupleSchemaRegistry;

import static org.junit.Assert.assertEquals;

public class BeanClassGeneratorTest
{

  protected class TestMeta extends TestWatcher
  {
    String generatedDir;

    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      generatedDir = "target/" + description.getClassName() + "/" + description.getMethodName();
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void test() throws IOException, JSONException, IllegalAccessException, InstantiationException,
    NoSuchFieldException, NoSuchMethodException, InvocationTargetException
  {
    String addressClassName = TupleSchemaRegistry.FQCN_PACKAGE + "Address_v1";

    TupleSchemaRegistry.Schema schema = new TupleSchemaRegistry.Schema();
    schema.addField("streetNumber", long.class);
    schema.addField("streetName", String.class);
    schema.addField("city", String.class);
    schema.addField("state", String.class);
    schema.addField("zip", String.class);

    byte[] beanClass = BeanClassGenerator.createAndWriteBeanClass(addressClassName, schema.fieldList);

    Class<?> clazz = ClassLoaderUtils.readBeanClass(addressClassName, beanClass);

    Object o = clazz.newInstance();
    Field f = clazz.getDeclaredField("streetNumber");
    Assert.assertNotNull(f);

    Method m = clazz.getDeclaredMethod("setStreetNumber", Long.class);
    m.invoke(o, 343L);

    m = clazz.getMethod("getStreetNumber");
    Long result = (Long)m.invoke(o);

    assertEquals("reflect getVal invoke", 343, result.longValue());
  }

  @Test
  public void testPrimitive() throws IOException, JSONException, IllegalAccessException, InstantiationException,
    NoSuchFieldException, NoSuchMethodException, InvocationTargetException
  {
    String addressClassName = TupleSchemaRegistry.FQCN_PACKAGE + "Energy_v1";

    TupleSchemaRegistry.Schema schema = new TupleSchemaRegistry.Schema();
    schema.addField("streetNumber", Integer.class);
    schema.addField("houseNumber", Long.class);
    schema.addField("condo", Boolean.class);
    schema.addField("water-usage", Float.class);
    schema.addField("electricity-usage", Double.class);
    schema.addField("startDate", Date.class);

    byte[] beanClass = BeanClassGenerator.createAndWriteBeanClass(addressClassName, schema.fieldList);

    Class<?> clazz = ClassLoaderUtils.readBeanClass(addressClassName, beanClass);

    Object o = clazz.newInstance();
    Field f = clazz.getDeclaredField("streetNumber");
    Assert.assertNotNull(f);

    //int setter and getter
    Method m = clazz.getDeclaredMethod("setStreetNumber", Integer.class);
    m.invoke(o, 343);
    m = clazz.getMethod("getStreetNumber");
    Integer result = (Integer)m.invoke(o);

    assertEquals("reflect getStreetNumber invoke", 343, result.intValue());

    //long setter and getter
    m = clazz.getDeclaredMethod("setHouseNumber", Long.class);
    m.invoke(o, 123L);
    m = clazz.getMethod("getHouseNumber");
    Long houseNum = (Long)m.invoke(o);

    assertEquals("reflect getHouseNumber invoke", 123L, houseNum.longValue());

    //boolean setter and getter
    m = clazz.getDeclaredMethod("setCondo", Boolean.class);
    m.invoke(o, true);
    m = clazz.getMethod("getCondo");
    Boolean isCondo = (Boolean)m.invoke(o);

    assertEquals("reflect getCondo invoke", true, isCondo);

    //float setter and getter
    m = clazz.getDeclaredMethod("setWater-usage", Float.class);
    m.invoke(o, 88.34F);
    m = clazz.getMethod("getWater-usage");
    Float waterUsage = (Float)m.invoke(o);

    assertEquals("reflect getWaterUsage invoke", 88.34F, waterUsage.floatValue(), 0);

    //double setter and getter
    m = clazz.getDeclaredMethod("setElectricity-usage", Double.class);
    m.invoke(o, 88.343243);
    m = clazz.getMethod("getElectricity-usage");
    Double electricityUsage = (Double)m.invoke(o);

    assertEquals("reflect getWaterUsage invoke", 88.343243, electricityUsage, 0);

    Date now = new Date();
    m = clazz.getDeclaredMethod("setStartDate", Date.class);
    m.invoke(o, now);

    m = clazz.getMethod("getStartDate");
    Date startDate = (Date)m.invoke(o);
    assertEquals("reflect getStartDate invoke", now, startDate);

    m = clazz.getMethod("getStartDateMs");
    long startDateMs = (long)m.invoke(o);
    assertEquals("reflect getStartDateMs invoke", now.getTime(), startDateMs, 0);

    m = clazz.getMethod("getStartDateSec");
    int startDateSec = (int)m.invoke(o);
    assertEquals("reflect getStartDateSec invoke", now.getTime() / 1000, startDateSec, 0);

    m = clazz.getDeclaredMethod("setStartDateMs", Long.class);
    m.invoke(o, now.getTime());

    m = clazz.getMethod("getStartDate");
    startDate = (Date)m.invoke(o);
    assertEquals("reflect getStartDate invoke", now, startDate);

    m = clazz.getMethod("getStartDateMs");
    startDateMs = (long)m.invoke(o);
    assertEquals("reflect getStartDateMs invoke", now.getTime(), startDateMs, 0);

    m = clazz.getMethod("getStartDateSec");
    startDateSec = (int)m.invoke(o);
    assertEquals("reflect getStartDateSec invoke", now.getTime() / 1000, startDateSec, 0);

    m = clazz.getDeclaredMethod("setStartDateSec", Integer.class);
    m.invoke(o, (int)(now.getTime() / 1000));

    now = new Date(now.getTime() / 1000 * 1000);
    m = clazz.getMethod("getStartDate");
    startDate = (Date)m.invoke(o);
    assertEquals("reflect getStartDate invoke", now, startDate);

    m = clazz.getMethod("getStartDateMs");
    startDateMs = (long)m.invoke(o);
    assertEquals("reflect getStartDateMs invoke", now.getTime(), startDateMs, 0);

    m = clazz.getMethod("getStartDateSec");
    startDateSec = (int)m.invoke(o);
    assertEquals("reflect getStartDateSec invoke", now.getTime() / 1000, startDateSec, 0);

  }
}
