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
package org.apache.apex.malhar.lib.window.accumulation;

import org.junit.Assert;
import org.junit.Test;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableFloat;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;

/**
 * Test for different Sum Accumulations.
 */
public class SumTest
{
  @Test
  public void SumTest()
  {
    SumInt si = new SumInt();
    SumLong sl = new SumLong();
    SumFloat sf = new SumFloat();
    SumDouble sd = new SumDouble();

    Assert.assertEquals(new MutableInt(10), si.accumulate(si.defaultAccumulatedValue(), 10));
    Assert.assertEquals(new MutableInt(11), si.accumulate(new MutableInt(1), 10));
    Assert.assertEquals(new MutableInt(22), si.merge(new MutableInt(1), new MutableInt(21)));

    Assert.assertEquals(new MutableLong(10L), sl.accumulate(sl.defaultAccumulatedValue(), 10L));
    Assert.assertEquals(new MutableLong(22L), sl.accumulate(new MutableLong(2L), 20L));
    Assert.assertEquals(new MutableLong(41L), sl.merge(new MutableLong(32L), new MutableLong(9L)));

    Assert.assertEquals(new MutableFloat(9.0F), sf.accumulate(sf.defaultAccumulatedValue(), 9.0F));
    Assert.assertEquals(new MutableFloat(22.5F), sf.accumulate(new MutableFloat(2.5F), 20F));
    Assert.assertEquals(new MutableFloat(41.0F), sf.merge(new MutableFloat(33.1F), new MutableFloat(7.9F)));

    Assert.assertEquals(new MutableDouble(9.0), sd.accumulate(sd.defaultAccumulatedValue(), 9.0));
    Assert.assertEquals(new MutableDouble(22.5), sd.accumulate(new MutableDouble(2.5), 20.0));
    Assert.assertEquals(new MutableDouble(41.0), sd.merge(new MutableDouble(33.1), new MutableDouble(7.9)));
  }
}
