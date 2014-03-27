/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.datamodel.operation;

import junit.framework.Assert;

import org.junit.Test;

public class CountOperationTest
{

  @Test
  public void testIntType()
  {
    Integer countV = new Integer(0);
    CountOperation<Integer> count = new CountOperation<Integer>();
    countV = count.compute(countV, 1);
    countV = count.compute(countV, 2);
    Assert.assertEquals(2, countV.intValue());
  }

  @Test
  public void testLongType()
  {
    Long countV = new Long(0);
    CountOperation<Long> count = new CountOperation<Long>();
    countV = count.compute(countV, null);
    countV = count.compute(countV, null);
    Assert.assertEquals(2l, countV.longValue());
  }

  @Test
  public void testDoubleType()
  {
    Double countV = new Double(0);
    CountOperation<Double> count = new CountOperation<Double>();
    countV = count.compute(countV, null);
    countV = count.compute(countV, null);
    Assert.assertEquals(2.0, countV.doubleValue());
  }

  @Test
  public void testFloatType()
  {
    Float countV = new Float(0);
    CountOperation<Float> count = new CountOperation<Float>();
    countV = count.compute(countV, null);
    countV = count.compute(countV, null);
    Assert.assertEquals(2.0f, countV.floatValue());
  }

}
