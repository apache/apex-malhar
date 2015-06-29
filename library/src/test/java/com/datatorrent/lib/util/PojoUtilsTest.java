/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.util;

import static com.datatorrent.lib.util.PojoUtils.createGetter;
import static com.datatorrent.lib.util.PojoUtils.createGetterBoolean;
import static com.datatorrent.lib.util.PojoUtils.createGetterByte;
import static com.datatorrent.lib.util.PojoUtils.createGetterChar;
import static com.datatorrent.lib.util.PojoUtils.createGetterDouble;
import static com.datatorrent.lib.util.PojoUtils.createGetterFloat;
import static com.datatorrent.lib.util.PojoUtils.createGetterInt;
import static com.datatorrent.lib.util.PojoUtils.createGetterLong;
import static com.datatorrent.lib.util.PojoUtils.createGetterShort;
import static com.datatorrent.lib.util.PojoUtils.createSetter;
import static com.datatorrent.lib.util.PojoUtils.createSetterBoolean;
import static com.datatorrent.lib.util.PojoUtils.createSetterByte;
import static com.datatorrent.lib.util.PojoUtils.createSetterChar;
import static com.datatorrent.lib.util.PojoUtils.createSetterDouble;
import static com.datatorrent.lib.util.PojoUtils.createSetterFloat;
import static com.datatorrent.lib.util.PojoUtils.createSetterInt;
import static com.datatorrent.lib.util.PojoUtils.createSetterLong;
import static com.datatorrent.lib.util.PojoUtils.createSetterShort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterByte;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.Getter;
import com.datatorrent.lib.util.PojoUtils.GetterShort;
import com.datatorrent.lib.util.PojoUtils.Setter;
import com.datatorrent.lib.util.PojoUtils.SetterBoolean;
import com.datatorrent.lib.util.PojoUtils.SetterByte;
import com.datatorrent.lib.util.PojoUtils.SetterInt;
import com.datatorrent.lib.util.PojoUtils.SetterLong;
import com.datatorrent.lib.util.PojoUtils.SetterShort;

import com.esotericsoftware.kryo.Kryo;


public class PojoUtilsTest
{

  private final Class<?> fqcn = TestObjAllTypes.class;
  private final Class<TestObjAllTypes.InnerObj> innerObjClass = TestObjAllTypes.InnerObj.class;
  private final TestObjAllTypes testObj = new TestObjAllTypes();
  private final TestObjAllTypes.InnerObj innerObj = testObj.innerObj;

  @Test
  public void testGetters() throws Exception
  {
    GetterBoolean<Object> getBoolean = createGetterBoolean(fqcn, "innerObj.boolVal");
    assertEquals(testObj.innerObj.isBoolVal(), getBoolean.get(testObj));

    GetterByte<Object> getByte = createGetterByte(fqcn, "innerObj.byteVal");
    assertEquals(testObj.innerObj.getByteVal(), getByte.get(testObj));

    GetterChar<Object> getChar = createGetterChar(fqcn, "innerObj.charVal");
    assertEquals(testObj.innerObj.getCharVal(), getChar.get(testObj));

    GetterShort<Object> getShort = createGetterShort(fqcn, "innerObj.shortVal");
    assertEquals(testObj.innerObj.getShortVal(), getShort.get(testObj));

    GetterInt<Object> getInt = createGetterInt(fqcn, "innerObj.intVal");
    assertEquals(testObj.innerObj.getIntVal(), getInt.get(testObj));

    GetterLong<Object> getLong = createGetterLong(fqcn, "innerObj.longVal");
    assertEquals(testObj.innerObj.getLongVal(), getLong.get(testObj));

    GetterFloat<Object> getFloat = createGetterFloat(fqcn, "innerObj.floatVal");
    assertEquals(testObj.innerObj.getFloatVal(), getFloat.get(testObj), 0);

    GetterDouble<Object> getDouble = createGetterDouble(fqcn, "innerObj.doubleVal");
    assertEquals(testObj.innerObj.getDoubleVal(), getDouble.get(testObj), 0);

    Getter<Object, String> getString = createGetter(fqcn, "innerObj.stringVal", String.class);
    assertEquals(testObj.innerObj.getStringVal(), getString.get(testObj));

    Getter<Object, Object> getObject = createGetter(fqcn, "innerObj.objVal", Object.class);
    assertEquals(testObj.innerObj.getObjVal(), getObject.get(testObj));

  }

  @Test
  public void testSerialization() throws Exception
  {
    GetterBoolean<Object> getBoolean = createGetterBoolean(fqcn, "innerObj.boolVal");
    TestUtils.clone(new Kryo(), getBoolean);
  }

  @Test
  public void testSettersBoolean() throws Exception
  {
    boolean boolVal = !innerObj.boolVal;
    createSetterBoolean(fqcn, "innerObj.boolVal").set(testObj, boolVal);
    assertEquals(boolVal, innerObj.boolVal);
    createSetterBoolean(innerObjClass, "boolVal").set(innerObj, !boolVal);
    assertEquals(!boolVal, innerObj.boolVal);
    createSetterBoolean(innerObjClass, "protectedBoolVal").set(innerObj, boolVal);
    assertEquals(boolVal, innerObj.protectedBoolVal);
    createSetterBoolean(innerObjClass, "privateBoolVal").set(innerObj, boolVal);
    assertEquals(boolVal, innerObj.isPrivateBoolVal());
  }

  @Test
  public void testSettersByte() throws Exception
  {
    byte byteVal = innerObj.byteVal;
    createSetterByte(fqcn, "innerObj.byteVal").set(testObj, ++byteVal);
    assertEquals(byteVal, innerObj.byteVal);
    createSetterByte(innerObjClass, "byteVal").set(innerObj, ++byteVal);
    assertEquals(byteVal, innerObj.byteVal);
    createSetterByte(innerObjClass, "protectedByteVal").set(innerObj, ++byteVal);
    assertEquals(byteVal, innerObj.protectedByteVal);
    createSetterByte(innerObjClass, "privateByteVal").set(innerObj, ++byteVal);
    assertEquals(byteVal, innerObj.getPrivateByteVal());
  }
  
  @Test
  public void testSetterChar() throws Exception
  {
    char charVal = innerObj.charVal;
    createSetterChar(fqcn, "innerObj.charVal").set(testObj, ++charVal);
    assertEquals(charVal, innerObj.charVal);
    createSetterChar(innerObjClass, "charVal").set(innerObj, ++charVal);
    assertEquals(charVal, innerObj.charVal);
    createSetterChar(innerObjClass, "protectedCharVal").set(innerObj, ++charVal);
    assertEquals(charVal, innerObj.protectedCharVal);
    createSetterChar(innerObjClass, "privateCharVal").set(innerObj, ++charVal);
    assertEquals(charVal, innerObj.getPrivateCharVal());
  }
  
  @Test
  public void testSetterShort() throws Exception
  {
    short shortVal = innerObj.shortVal;
    createSetterShort(fqcn, "innerObj.shortVal").set(testObj, ++shortVal);
    assertEquals(shortVal, innerObj.shortVal);
    createSetterShort(innerObjClass, "shortVal").set(innerObj, ++shortVal);
    assertEquals(shortVal, innerObj.shortVal);
    createSetterShort(innerObjClass, "protectedShortVal").set(innerObj, ++shortVal);
    assertEquals(shortVal, innerObj.protectedShortVal);
    createSetterShort(innerObjClass, "privateShortVal").set(innerObj, ++shortVal);
    assertEquals(shortVal, innerObj.getPrivateShortVal());
  }

  @Test
  public void testSetterInt() throws Exception
  {
    int intVal = innerObj.intVal;
    PojoUtils.createSetterInt(fqcn, "innerObj.intVal").set(testObj, ++intVal);
    assertEquals(intVal, innerObj.intVal);
    createSetterInt(innerObjClass, "intVal").set(innerObj, ++intVal);
    assertEquals(intVal, innerObj.intVal);
    createSetterInt(innerObjClass, "protectedIntVal").set(innerObj, ++intVal);
    assertEquals(intVal, innerObj.protectedIntVal);
    createSetterInt(innerObjClass, "privateIntVal").set(innerObj, ++intVal);
    assertEquals(intVal, innerObj.getPrivateIntVal());
  }

  @Test
  public void testSetterLong() throws Exception
  {
    long longVal = innerObj.longVal;
    PojoUtils.createSetterLong(fqcn, "innerObj.longVal").set(testObj, ++longVal);
    assertEquals(longVal, innerObj.longVal);
    createSetterLong(innerObjClass, "longVal").set(innerObj, ++longVal);
    assertEquals(longVal, innerObj.longVal);
    createSetterLong(innerObjClass, "protectedLongVal").set(innerObj, ++longVal);
    assertEquals(longVal, innerObj.protectedLongVal);
    createSetterLong(innerObjClass, "privateLongVal").set(innerObj, ++longVal);
    assertEquals(longVal, innerObj.getPrivateLongVal());
  }

  @Test
  public void testSetterString() throws Exception
  {
    String string = innerObj.stringVal.concat("test");
    createSetter(fqcn, "innerObj.stringVal", String.class).set(testObj, string);
    assertSame(string, innerObj.stringVal);
    createSetter(innerObjClass, "stringVal", String.class).set(innerObj, string = string.concat("more test"));
    assertEquals(string, innerObj.stringVal);
    createSetter(innerObjClass, "protectedStringVal", String.class).set(innerObj, string = string.concat("and more test"));
    assertSame(string, innerObj.protectedStringVal);
    createSetter(innerObjClass, "privateStringVal", String.class).set(innerObj, string = string.concat("and even more test"));
    assertSame(string, innerObj.getPrivateStringVal());
  }

  @Test
  public void testSetterFloat() throws Exception
  {
    float floatVal = innerObj.floatVal;
    createSetterFloat(fqcn, "innerObj.floatVal").set(testObj, ++floatVal);
    assertEquals(floatVal, innerObj.floatVal, 0);
    createSetterFloat(innerObjClass, "floatVal").set(innerObj, ++floatVal);
    assertEquals(floatVal, innerObj.floatVal, 0);
    createSetterFloat(innerObjClass, "protectedFloatVal").set(innerObj, ++floatVal);
    assertEquals(floatVal, innerObj.protectedFloatVal, 0);
    createSetterFloat(innerObjClass, "privateFloatVal").set(innerObj, ++floatVal);
    assertEquals(floatVal, innerObj.getPrivateFloatVal(), 0);
  }

  @Test
  public void testSetterDouble() throws Exception
  {
    double doubleVal = innerObj.doubleVal;
    createSetterDouble(fqcn, "innerObj.doubleVal").set(testObj, ++doubleVal);
    assertEquals(doubleVal, innerObj.doubleVal, 0);
    createSetterDouble(innerObjClass, "doubleVal").set(innerObj, ++doubleVal);
    assertEquals(doubleVal, innerObj.doubleVal, 0);
    createSetterDouble(innerObjClass, "protectedDoubleVal").set(innerObj, ++doubleVal);
    assertEquals(doubleVal, innerObj.protectedDoubleVal, 0);
    createSetterDouble(innerObjClass, "privateDoubleVal").set(innerObj, ++doubleVal);
    assertEquals(doubleVal, innerObj.getPrivateDoubleVal(), 0);
  }

  @Test
  public void testSetterObject() throws Exception
  {
    createSetter(fqcn, "innerObj.objVal", Object.class).set(testObj, fqcn);
    assertSame(fqcn, innerObj.objVal);
    createSetter(innerObjClass, "objVal", Object.class).set(innerObj, innerObjClass);
    assertSame(innerObjClass, innerObj.objVal);
    createSetter(innerObjClass, "protectedObjVal", Object.class).set(innerObj, innerObjClass);
    assertSame(innerObjClass, innerObj.protectedObjVal);
    createSetter(innerObjClass, "privateObjVal", Object.class).set(innerObj, innerObjClass);
    assertSame(innerObjClass, innerObj.getPrivateObjVal());
  }

  public static class TestPojo
  {
    public static final String INT_FIELD_NAME = "intField";
    public int intField;
    @SuppressWarnings("unused")
    private int privateIntField;

    public TestPojo(int intVal)
    {
      intField = intVal;
    }

    public int getIntVal()
    {
      return intField;
    }

    public boolean isBoolVal()
    {
      return true;
    }

    @SuppressWarnings("unused")
    public void setIntVal(int intVal)
    {
      intField = intVal;
    }

    @SuppressWarnings("unused")
    public void setIntVal(Integer intVal) {
      intField = intVal;
    }

    @SuppressWarnings("unused")
    public void setBoolVal(boolean boolVal)
    {
      throw new UnsupportedOperationException("setting boolean is not supported");
    }

    @SuppressWarnings("unused")
    private void setPrivateInt(final int intVal)
    {
      throw new UnsupportedOperationException("not the right method");
    }

    @SuppressWarnings("unused")
    public void setPrivateInt(final int intVal, final int anotherIntVal)
    {
      throw new UnsupportedOperationException("not the right method");
    }

  }

  @Test
  public void testGetterOrFieldExpression()
  {
    TestPojo testObj = new TestPojo(1);

    Class<?> testObjClass = testObj.getClass();

    Getter<Object, Object> getObject = createGetter(testObjClass, "intVal", Object.class);
    assertEquals(testObj.getIntVal(), getObject.get(testObj));
    assertEquals(testObj.getIntVal(), createGetterInt(testObjClass, "intVal").get(testObj));

    getObject = createGetter(testObjClass, "intField", Object.class);
    assertEquals(testObj.intField, getObject.get(testObj));
    assertEquals(testObj.intField, createGetterInt(testObjClass, "intField").get(testObj));

    getObject = PojoUtils.createGetter(testObjClass, "boolVal", Object.class);
    assertEquals(testObj.isBoolVal(), getObject.get(testObj));
    assertEquals(testObj.isBoolVal(), createGetterBoolean(testObjClass, "boolVal").get(testObj));

  }

  @Test
  public void testComplexGetter()
  {
    TestPojo testPojo = new TestPojo(1);
    final Class<?> testPojoClass = testPojo.getClass();
    GetterInt<Object> getterInt = createGetterInt(testPojoClass, "{$}.getIntVal() + {$}.intField");
    assertEquals(testPojo.getIntVal() + testPojo.intField, getterInt.get(testPojo));
  }

  @Test
  public void testComplexSetter()
  {
    TestPojo testPojo = new TestPojo(1);
    Class<?> testPojoClass = testPojo.getClass();
    SetterInt<Object> setterInt = createSetterInt(testPojoClass, "{$}.setIntVal({$}.getIntVal() + {#})");
    setterInt.set(testPojo, 20);
    Setter<Object, String> setterString = createSetter(testPojoClass, "{$}.setIntVal(Integer.valueOf({#}))", String.class);
    setterString.set(testPojo, "20");
  }

  @Test
  @SuppressWarnings("UnnecessaryBoxing")
  public void testSetterOrFieldExpression()
  {
    TestPojo testPojo = new TestPojo(0);
    final Class<?> testPojoClass = testPojo.getClass();
    SetterInt<Object> setterInt = createSetterInt(testPojoClass, TestPojo.INT_FIELD_NAME);
    setterInt.set(testPojo, 1);
    assertEquals(1, testPojo.intField);

    setterInt = createSetterInt(testPojoClass, "intVal");
    setterInt.set(testPojo, 2);
    assertEquals(2, testPojo.getIntVal());

    SetterByte<Object> setterByte = createSetterByte(testPojoClass, TestPojo.INT_FIELD_NAME);
    setterByte.set(testPojo, (byte)3);
    assertEquals(3, testPojo.intField);

    SetterShort<Object> setterShort = createSetterShort(testPojoClass, TestPojo.INT_FIELD_NAME);
    setterShort.set(testPojo, (short)4);
    assertEquals(4, testPojo.intField);

    try {
      @SuppressWarnings("unused")
      SetterLong<Object> setterLong = createSetterLong(testPojoClass, TestPojo.INT_FIELD_NAME);
      fail("long can't be assigned to the int field");
    }
    catch (Exception ignored) {
    }

    Setter<Object, Integer> setterInteger = createSetter(testPojoClass, TestPojo.INT_FIELD_NAME, Integer.class);
    setterInteger.set(testPojo, Integer.valueOf(5));
    assertEquals(5, testPojo.intField);

  }

  @Test (expected = UnsupportedOperationException.class)
  public void testExceptionInSetter()
  {
    final Class<?> testPojoClass = TestPojo.class;
    SetterBoolean<Object> setterBoolean = createSetterBoolean(testPojoClass, "boolVal");
    setterBoolean.set(new TestPojo(3), false);
  }

  @Test (expected = RuntimeException.class)
  public void testPrivateField()
  {
    final Class<?> testPojoClass = TestPojo.class;
    @SuppressWarnings("unused")
    SetterInt<Object> setterInt = createSetterInt(testPojoClass, "privateIntField");
  }

  @Test (expected = RuntimeException.class)
  public void testWrongSetterMethod()
  {
    final Class<?> testPojoClass = TestPojo.class;
    @SuppressWarnings("unused")
    SetterInt<Object> setterInt = createSetterInt(testPojoClass, "privateInt");
  }
}
