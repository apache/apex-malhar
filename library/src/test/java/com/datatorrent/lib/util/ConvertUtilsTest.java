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

import com.datatorrent.lib.util.ConvertUtils;
import com.datatorrent.lib.util.ConvertUtils.GetterBoolean;
import com.datatorrent.lib.util.ConvertUtils.GetterByte;
import com.datatorrent.lib.util.ConvertUtils.GetterChar;
import com.datatorrent.lib.util.ConvertUtils.GetterDouble;
import com.datatorrent.lib.util.ConvertUtils.GetterFloat;
import com.datatorrent.lib.util.ConvertUtils.GetterInt;
import com.datatorrent.lib.util.ConvertUtils.GetterLong;
import com.datatorrent.lib.util.ConvertUtils.GetterObject;
import com.datatorrent.lib.util.ConvertUtils.GetterShort;
import com.datatorrent.lib.util.ConvertUtils.GetterString;
import com.esotericsoftware.kryo.Kryo;

public class ConvertUtilsTest
{
  @Test
  public void simpleTest() throws Exception
  {
    Class<?> fqcn = TestObjAllTypes.class;
    TestObjAllTypes testObj = new TestObjAllTypes();

    GetterBoolean getBoolean = ConvertUtils.createExpressionGetterBoolean(fqcn, "innerObj.boolVal");
    Assert.assertEquals(testObj.innerObj.isBoolVal(), getBoolean.get(testObj));

    GetterByte getByte = ConvertUtils.createExpressionGetterByte(fqcn, "innerObj.byteVal");
    Assert.assertEquals(testObj.innerObj.getByteVal(), getByte.get(testObj));

    GetterChar getChar = ConvertUtils.createExpressionGetterChar(fqcn, "innerObj.charVal");
    Assert.assertEquals(testObj.innerObj.getCharVal(), getChar.get(testObj));

    GetterString getString = ConvertUtils.createExpressionGetterString(fqcn, "innerObj.stringVal");
    Assert.assertEquals(testObj.innerObj.getStringVal(), getString.get(testObj));

    GetterShort getShort = ConvertUtils.createExpressionGetterShort(fqcn, "innerObj.shortVal");
    Assert.assertEquals(testObj.innerObj.getShortVal(), getShort.get(testObj));

    GetterInt getInt = ConvertUtils.createExpressionGetterInt(fqcn, "innerObj.intVal");
    Assert.assertEquals(testObj.innerObj.getIntVal(), getInt.get(testObj));

    GetterLong getLong = ConvertUtils.createExpressionGetterLong(fqcn, "innerObj.longVal");
    Assert.assertEquals(testObj.innerObj.getLongVal(), getLong.get(testObj));

    GetterFloat getFloat = ConvertUtils.createExpressionGetterFloat(fqcn, "innerObj.floatVal");
    Assert.assertEquals(testObj.innerObj.getFloatVal(), getFloat.get(testObj), 0);

    GetterDouble getDouble = ConvertUtils.createExpressionGetterDouble(fqcn, "innerObj.doubleVal");
    Assert.assertEquals(testObj.innerObj.getDoubleVal(), getDouble.get(testObj), 0);

    GetterObject getObject = ConvertUtils.createExpressionGetterObject(fqcn, "innerObj.objVal");
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

    String expr = ConvertUtils.getSingleFieldExpression(testObj.getClass(), "intVal");
    GetterObject getObject = ConvertUtils.createExpressionGetterObject(testObj.getClass(), expr);
    Assert.assertEquals(testObj.getIntVal(), getObject.get(testObj));

    expr = ConvertUtils.getSingleFieldExpression(testObj.getClass(), "intField");
    getObject = ConvertUtils.createExpressionGetterObject(testObj.getClass(), expr);
    Assert.assertEquals(testObj.intField, getObject.get(testObj));

    expr = ConvertUtils.getSingleFieldExpression(testObj.getClass(), "boolVal");
    getObject = ConvertUtils.createExpressionGetterObject(testObj.getClass(), expr);
    Assert.assertEquals(testObj.isBoolVal(), getObject.get(testObj));

  }

}
