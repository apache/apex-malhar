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
 */
public class SumOperationTest
{
  @Test
  public void test()
  {
    Object sumV = 0.0;
    SumOperation sum = new SumOperation();
    sumV = sum.compute(sumV, 1);
    sumV = sum.compute(sumV, 2);
    Assert.assertEquals(3.0, sumV);
  }

  @Test
  public void testNulls()
  {
    Object sumV = null;
    SumOperation sum = new SumOperation();
    sumV = sum.compute(sumV, 1);
    sumV = sum.compute(sumV, null);
    Assert.assertEquals(1.0, sumV);
  }

}

