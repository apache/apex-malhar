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
package org.apache.apex.malhar.lib.appdata.schemas;

import org.junit.Assert;
import org.junit.Test;

public class CustomTimeBucketTest
{
  @Test
  public void stringCreationTest()
  {
    CustomTimeBucket customTimeBucket = new CustomTimeBucket("5m");

    Assert.assertEquals(5L, customTimeBucket.getCount());
    Assert.assertEquals("5m", customTimeBucket.getText());
    Assert.assertEquals(TimeBucket.MINUTE, customTimeBucket.getTimeBucket());
  }

  @Test
  public void stringCreationTest2()
  {
    CustomTimeBucket customTimeBucket = new CustomTimeBucket("6h");

    Assert.assertEquals(6L, customTimeBucket.getCount());
    Assert.assertEquals("6h", customTimeBucket.getText());
    Assert.assertEquals(TimeBucket.HOUR, customTimeBucket.getTimeBucket());
  }

  @Test
  public void testToMillis()
  {
    CustomTimeBucket customTimeBucket = new CustomTimeBucket("5m");

    Assert.assertEquals(5L * 60L * 1000L, customTimeBucket.getNumMillis());
  }

  @Test
  public void roundDownTest()
  {
    CustomTimeBucket customTimeBucket = new CustomTimeBucket("5m");

    long expected = 5 * 60 * 1000;
    long val = expected + 300;

    Assert.assertEquals(expected, customTimeBucket.roundDown(val));
  }

  @Test
  public void compareTest()
  {
    CustomTimeBucket bigger = new CustomTimeBucket("180m");
    CustomTimeBucket smaller = new CustomTimeBucket("2h");

    Assert.assertTrue(bigger.compareTo(bigger) == 0);
    Assert.assertTrue(smaller.compareTo(smaller) == 0);

    Assert.assertTrue(bigger.compareTo(smaller) > 0);
    Assert.assertTrue(smaller.compareTo(bigger) < 0);

    smaller = new CustomTimeBucket("91m");

    Assert.assertTrue(bigger.compareTo(smaller) > 0);
    Assert.assertTrue(smaller.compareTo(bigger) < 0);
  }
}
