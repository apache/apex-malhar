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

package com.datatorrent.lib.appbuilder.convert.pojo;

import com.datatorrent.lib.appbuilder.convert.pojo.TestObj.InnerObj;
import org.junit.Assert;
import org.junit.Test;

public class GetterBooleanTest
{
  @Test
  public void simpleTest()
  {
    GetterBooleanExpression getter = new GetterBooleanExpression(TestObj.class.getName(),
                                             "getInnerObj().getTestField()");

    boolean expected = false;

    TestObj<Boolean> testObj = new TestObj<Boolean>();
    testObj.setInnerObj(new InnerObj<Boolean>());
    testObj.getInnerObj().setTestField(expected);

    boolean value = getter.get(testObj);

    Assert.assertEquals("The expected must equal actual", expected, value);

    expected = true;
    testObj.getInnerObj().setTestField(expected);

    value = getter.get(testObj);

    Assert.assertEquals("The expected must equal actual", expected, value);
  }
}
