/*
 * Copyright (c) 2015 DataTorrent
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

package com.datatorrent.lib.appdata.schemas;

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
}
