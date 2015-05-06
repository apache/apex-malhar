/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.util;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.util.PojoUtils.GetterBoolean;
import com.datatorrent.lib.util.PojoUtils.GetterByte;
import com.datatorrent.lib.util.PojoUtils.GetterChar;
import com.datatorrent.lib.util.PojoUtils.GetterDouble;
import com.datatorrent.lib.util.PojoUtils.GetterFloat;
import com.datatorrent.lib.util.PojoUtils.GetterInt;
import com.datatorrent.lib.util.PojoUtils.GetterLong;
import com.datatorrent.lib.util.PojoUtils.GetterObject;
import com.datatorrent.lib.util.PojoUtils.GetterShort;
import com.datatorrent.lib.util.PojoUtils.GetterString;
import com.esotericsoftware.kryo.Kryo;

public class PojoUtilsTest
{
  @Test
  public void simpleTest() throws Exception
  {
    Class<?> fqcn = TestObjAllTypes.class;
    TestObjAllTypes testObj = new TestObjAllTypes();

    GetterBoolean getBoolean = PojoUtils.createGetterBoolean(fqcn, "innerObj.boolVal");
    Assert.assertEquals(testObj.innerObj.isBoolVal(), getBoolean.get(testObj));

    GetterByte getByte = PojoUtils.createGetterByte(fqcn, "innerObj.byteVal");
    Assert.assertEquals(testObj.innerObj.getByteVal(), getByte.get(testObj));

    GetterChar getChar = PojoUtils.createGetterChar(fqcn, "innerObj.charVal");
    Assert.assertEquals(testObj.innerObj.getCharVal(), getChar.get(testObj));

    GetterString getString = PojoUtils.createGetterString(fqcn, "innerObj.stringVal");
    Assert.assertEquals(testObj.innerObj.getStringVal(), getString.get(testObj));

    GetterShort getShort = PojoUtils.createGetterShort(fqcn, "innerObj.shortVal");
    Assert.assertEquals(testObj.innerObj.getShortVal(), getShort.get(testObj));

    GetterInt getInt = PojoUtils.createGetterInt(fqcn, "innerObj.intVal");
    Assert.assertEquals(testObj.innerObj.getIntVal(), getInt.get(testObj));

    GetterLong getLong = PojoUtils.createExpressionGetterLong(fqcn, "innerObj.longVal");
    Assert.assertEquals(testObj.innerObj.getLongVal(), getLong.get(testObj));

    GetterFloat getFloat = PojoUtils.createGetterFloat(fqcn, "innerObj.floatVal");
    Assert.assertEquals(testObj.innerObj.getFloatVal(), getFloat.get(testObj), 0);

    GetterDouble getDouble = PojoUtils.createGetterDouble(fqcn, "innerObj.doubleVal");
    Assert.assertEquals(testObj.innerObj.getDoubleVal(), getDouble.get(testObj), 0);

    GetterObject getObject = PojoUtils.createGetterObject(fqcn, "innerObj.objVal");
    Assert.assertEquals(testObj.innerObj.getObjVal(), getObject.get(testObj));

    //Check serialization
    TestUtils.clone(new Kryo(), getBoolean);

  }

  public static class TestPojo
  {
    public int intField = 1;

    public int getIntVal() {
      return 2;
    };

    public boolean isBoolVal()
    {
      return true;
    };

  }

  @Test
  public void testGetterOrFieldExpression()
  {
    TestPojo testObj = new TestPojo();

    String expr = PojoUtils.getSingleFieldExpression(testObj.getClass(), "intVal");
    GetterObject getObject = PojoUtils.createGetterObject(testObj.getClass(), expr);
    Assert.assertEquals(testObj.getIntVal(), getObject.get(testObj));

    expr = PojoUtils.getSingleFieldExpression(testObj.getClass(), "intField");
    getObject = PojoUtils.createGetterObject(testObj.getClass(), expr);
    Assert.assertEquals(testObj.intField, getObject.get(testObj));

    expr = PojoUtils.getSingleFieldExpression(testObj.getClass(), "boolVal");
    getObject = PojoUtils.createGetterObject(testObj.getClass(), expr);
    Assert.assertEquals(testObj.isBoolVal(), getObject.get(testObj));

  }

}
