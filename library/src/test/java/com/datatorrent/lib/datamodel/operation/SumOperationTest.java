
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

/**
 * Test case for SumOperation
 * 
 */
public class SumOperationTest
{
  @Test
  public void testIntType()
  {
    Integer sumV = new Integer(0);
    SumOperation<Integer, Integer> sum = new SumOperation<Integer,Integer>();
    sumV = sum.compute(sumV,1);
    sumV = sum.compute(sumV,2);
    Assert.assertEquals(3, sumV.intValue());
  }

  @Test
  public void testLongType()
  {
    Long sumV = new Long(0);
    SumOperation<Long, Integer> sum = new SumOperation<Long,Integer>();
    sumV = sum.compute(sumV,1);
    sumV = sum.compute(sumV,2);
    Assert.assertEquals(3l, sumV.longValue());
  }

  @Test
  public void testDoubleType()
  {
    Double sumV = new Double(0);
    SumOperation<Double, Double> sum = new SumOperation<Double, Double>();
    sumV = sum.compute(sumV,1.0);
    sumV = sum.compute(sumV,2.3);
    Assert.assertEquals(3.3, sumV.doubleValue());
  }

  @Test
  public void testFloatType()
  {
    Float sumV = new Float(0);
    SumOperation<Float, Double> sum = new SumOperation<Float, Double>();
    sumV = sum.compute(sumV,1.0);
    sumV = sum.compute(sumV,2.3);
    Assert.assertEquals(3.3f, sumV.floatValue());
  }
}
