/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.gateway.schema;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.gateway.resources.ws.v2.ClassSchemasResource;

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
    File addressFile = new File("src/test/resources/schemas/Address.json");
    String addressSchema = FileUtils.readFileToString(addressFile);

    JSONObject addressObj = new JSONObject(addressSchema);
    String addressClassName = ClassSchemasResource.PACKAGE_GENERATED_CLASSES + "Address_v1";
    Path addressClassPath = new Path(testMeta.generatedDir + "/Address_v1.class");
    FileSystem fs = FileSystem.newInstance(addressClassPath.toUri(), new Configuration());
    FSDataOutputStream outputStream = fs.create(addressClassPath);

    BeanClassGenerator.createAndWriteBeanClass(addressClassName, addressObj, outputStream);

    Class<?> clazz = BeanClassGenerator.readBeanClass(addressClassName, fs.open(addressClassPath));

    Object o = clazz.newInstance();
    Field f = clazz.getDeclaredField("streetNumber");
    Assert.assertNotNull(f);

    Method m = clazz.getDeclaredMethod("setStreetNumber", Long.class);
    m.invoke(o, 343L);

    m = clazz.getMethod("getStreetNumber");
    Long result = (Long) m.invoke(o);

    assertEquals("reflect getVal invoke", 343, result.longValue());
  }

  @Test
  public void testPrimitive() throws IOException, JSONException, IllegalAccessException, InstantiationException,
    NoSuchFieldException, NoSuchMethodException, InvocationTargetException
  {
    File addressFile = new File("src/test/resources/schemas/Energy.json");
    String addressSchema = FileUtils.readFileToString(addressFile);

    JSONObject addressObj = new JSONObject(addressSchema);
    String addressClassName = ClassSchemasResource.PACKAGE_GENERATED_CLASSES + "Energy_v1";
    Path addressClassPath = new Path(testMeta.generatedDir + "/Energy_v1.class");
    FileSystem fs = FileSystem.newInstance(addressClassPath.toUri(), new Configuration());
    FSDataOutputStream outputStream = fs.create(addressClassPath);

    BeanClassGenerator.createAndWriteBeanClass(addressClassName, addressObj, outputStream);

    Class<?> clazz = BeanClassGenerator.readBeanClass(addressClassName, fs.open(addressClassPath));

    Object o = clazz.newInstance();
    Field f = clazz.getDeclaredField("streetNumber");
    Assert.assertNotNull(f);

    //int setter and getter
    Method m = clazz.getDeclaredMethod("setStreetNumber", int.class);
    m.invoke(o, 343);
    m = clazz.getMethod("getStreetNumber");
    Integer result = (Integer) m.invoke(o);

    assertEquals("reflect getStreetNumber invoke", 343, result.intValue());

    //long setter and getter
    m = clazz.getDeclaredMethod("setHouseNumber", long.class);
    m.invoke(o, 123L);
    m = clazz.getMethod("getHouseNumber");
    Long houseNum = (Long) m.invoke(o);

    assertEquals("reflect getHouseNumber invoke", 123L, houseNum.longValue());

    //boolean setter and getter
    m = clazz.getDeclaredMethod("setCondo", boolean.class);
    m.invoke(o, true);
    m = clazz.getMethod("getCondo");
    Boolean isCondo = (Boolean) m.invoke(o);

    assertEquals("reflect getCondo invoke", true, isCondo);

    //float setter and getter
    m = clazz.getDeclaredMethod("setWater-usage", float.class);
    m.invoke(o, 88.34F);
    m = clazz.getMethod("getWater-usage");
    Float waterUsage = (Float) m.invoke(o);

    assertEquals("reflect getWaterUsage invoke", 88.34F, waterUsage.floatValue(), 0);

    //double setter and getter
    m = clazz.getDeclaredMethod("setElectricity-usage", double.class);
    m.invoke(o, 88.343243);
    m = clazz.getMethod("getElectricity-usage");
    Double electricityUsage = (Double) m.invoke(o);

    assertEquals("reflect getWaterUsage invoke", 88.343243, electricityUsage, 0);
  }
}
