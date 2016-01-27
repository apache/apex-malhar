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
    m = clazz.getMethod("isCondo");
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

  @Test
  public void testToString() throws IOException, JSONException, InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException
  {
    File testFile = new File("src/test/resources/schemas/TestSchema.json");
    String testSchema = FileUtils.readFileToString(testFile);

    JSONObject testSchemaObj = new JSONObject(testSchema);
    String testSchemaClassName = ClassSchemasResource.PACKAGE_GENERATED_CLASSES + "TestSchema_v1";
    Path testSchemaClassPath = new Path(testMeta.generatedDir + "/TestSchema_v1.class");
    FileSystem fs = FileSystem.newInstance(testSchemaClassPath.toUri(), new Configuration());
    FSDataOutputStream outputStream = fs.create(testSchemaClassPath);

    BeanClassGenerator.createAndWriteBeanClass(testSchemaClassName, testSchemaObj, outputStream);

    Class<?> clazz = BeanClassGenerator.readBeanClass(testSchemaClassName, fs.open(testSchemaClassPath));
    Object o = clazz.newInstance();

    Method m = clazz.getDeclaredMethod("setVString1", String.class);
    m.invoke(o, "vString1");
    m = clazz.getDeclaredMethod("setVString2", String.class);
    m.invoke(o, "vString2");
    m = clazz.getDeclaredMethod("setVChar1", char.class);
    m.invoke(o, '1');
    m = clazz.getDeclaredMethod("setVChar2", char.class);
    m.invoke(o, '2');
    m = clazz.getMethod("toString");
    String actualString = (String) m.invoke(o);
    String expectedString = "com/datatorrent/beans/generated/TestSchema_v1{vString1=vString1, vLong1=0, vInt1=0, vBool1=false, vString2=vString2, vShort2=0, vFloat1=0.0, vDouble2=0.0, vChar1=1, vLong2=0, vByte1=0, vShort1=0, vInt2=0, vDouble1=0.0, vFloat2=0.0, vByte2=0, vBool2=false, vChar2=2}";
    Assert.assertTrue(actualString.equals(expectedString));
  }

  @Test
  public void testHashCode() throws IOException, JSONException, InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException
  {
    File testFile = new File("src/test/resources/schemas/TestSchema.json");
    String testSchema = FileUtils.readFileToString(testFile);

    JSONObject testSchemaObj = new JSONObject(testSchema);
    String testSchemaClassName = ClassSchemasResource.PACKAGE_GENERATED_CLASSES + "TestSchema_v1";
    Path testSchemaClassPath = new Path(testMeta.generatedDir + "/TestSchema_v1.class");
    FileSystem fs = FileSystem.newInstance(testSchemaClassPath.toUri(), new Configuration());
    FSDataOutputStream outputStream = fs.create(testSchemaClassPath);

    BeanClassGenerator.createAndWriteBeanClass(testSchemaClassName, testSchemaObj, outputStream);

    Class<?> clazz = BeanClassGenerator.readBeanClass(testSchemaClassName, fs.open(testSchemaClassPath));
    Object o = clazz.newInstance();

    Method m = clazz.getDeclaredMethod("setVString1", String.class);
    m.invoke(o, "vString1");
    m = clazz.getDeclaredMethod("setVString2", String.class);
    m.invoke(o, "vString2");
    m = clazz.getDeclaredMethod("setVChar1", char.class);
    m.invoke(o, '1');
    m = clazz.getDeclaredMethod("setVChar2", char.class);
    m.invoke(o, '2');

    m = clazz.getMethod("hashCode");
    Integer actualHashCode = (Integer) m.invoke(o);
    int expectedHashCode = 1086553467;
    Assert.assertEquals(expectedHashCode, actualHashCode.intValue());

    m = clazz.getDeclaredMethod("setVLong1", long.class);
    m.invoke(o, 234972498);
    m = clazz.getMethod("hashCode");
    actualHashCode = (Integer) m.invoke(o);
    expectedHashCode = 2041226189;
    Assert.assertEquals(expectedHashCode, actualHashCode.intValue());

    m = clazz.getDeclaredMethod("setVBool1", boolean.class);
    m.invoke(o, true);
    m = clazz.getMethod("hashCode");
    actualHashCode = (Integer) m.invoke(o);
    expectedHashCode = 761736999;
    Assert.assertEquals(expectedHashCode, actualHashCode.intValue());

    m = clazz.getDeclaredMethod("setVString2", String.class);
    m.invoke(o, "TestingHashCode");
    m = clazz.getMethod("hashCode");
    actualHashCode = (Integer) m.invoke(o);
    expectedHashCode = 843404263;
    Assert.assertEquals(expectedHashCode, actualHashCode.intValue());
  }

  @Test
  public void testEquals() throws IOException, JSONException, InstantiationException, IllegalAccessException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException
  {
    File testFile = new File("src/test/resources/schemas/TestSchema.json");
    String testSchema = FileUtils.readFileToString(testFile);

    JSONObject testSchemaObj = new JSONObject(testSchema);
    String testSchemaClassName = ClassSchemasResource.PACKAGE_GENERATED_CLASSES + "TestSchema_v1";
    Path testSchemaClassPath = new Path(testMeta.generatedDir + "/TestSchema_v1.class");
    FileSystem fs = FileSystem.newInstance(testSchemaClassPath.toUri(), new Configuration());
    FSDataOutputStream outputStream = fs.create(testSchemaClassPath);

    BeanClassGenerator.createAndWriteBeanClass(testSchemaClassName, testSchemaObj, outputStream);

    Class<?> clazz = BeanClassGenerator.readBeanClass(testSchemaClassName, fs.open(testSchemaClassPath));

    Object o1 = clazz.newInstance();
    Method m = clazz.getDeclaredMethod("setVString1", String.class);
    m.invoke(o1, "vString1");
    m = clazz.getDeclaredMethod("setVString2", String.class);
    m.invoke(o1, "vString2");
    m = clazz.getDeclaredMethod("setVChar1", char.class);
    m.invoke(o1, '1');
    m = clazz.getDeclaredMethod("setVChar2", char.class);
    m.invoke(o1, '2');

    Object o2 = clazz.newInstance();
    m = clazz.getDeclaredMethod("setVString1", String.class);
    m.invoke(o2, "vString1");
    m = clazz.getDeclaredMethod("setVString2", String.class);
    m.invoke(o2, "vString2");
    m = clazz.getDeclaredMethod("setVChar1", char.class);
    m.invoke(o2, '1');
    m = clazz.getDeclaredMethod("setVChar2", char.class);
    m.invoke(o2, '2');

    m = clazz.getDeclaredMethod("equals", Object.class);
    Boolean b = (Boolean) m.invoke(o1, o2);
    Assert.assertTrue(b.booleanValue());

    m = clazz.getDeclaredMethod("setVInt1", int.class);
    m.invoke(o1, 123);
    m.invoke(o2, 321);
    m = clazz.getDeclaredMethod("setVInt2", int.class);
    m.invoke(o1, 321);
    m.invoke(o2, 123);
    m = clazz.getDeclaredMethod("equals", Object.class);
    b = (Boolean) m.invoke(o1, o2);
    Assert.assertFalse(b.booleanValue());

    m = clazz.getDeclaredMethod("setVInt1", int.class);
    m.invoke(o2, 123);
    m = clazz.getDeclaredMethod("setVInt2", int.class);
    m.invoke(o2, 321);
    m = clazz.getDeclaredMethod("equals", Object.class);
    b = (Boolean) m.invoke(o1, o2);
    Assert.assertTrue(b.booleanValue());

    m = clazz.getDeclaredMethod("setVString1", String.class);
    m.invoke(o2, "abcdefg");
    m = clazz.getDeclaredMethod("equals", Object.class);
    b = (Boolean) m.invoke(o1, o2);
    Assert.assertFalse(b.booleanValue());

    m = clazz.getDeclaredMethod("setVString1", String.class);
    m.invoke(o2, "vString1");
    m = clazz.getDeclaredMethod("equals", Object.class);
    b = (Boolean) m.invoke(o1, o2);
    Assert.assertTrue(b.booleanValue());
  }
}
