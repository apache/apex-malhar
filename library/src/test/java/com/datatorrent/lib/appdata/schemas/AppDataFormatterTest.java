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

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import org.junit.Assert;
import org.junit.Test;

public class AppDataFormatterTest
{
  @Test
  public void serializationTest() throws Exception
  {
    TestUtils.clone(new Kryo(), new AppDataFormatter());
  }

  @Test
  public void continuousFormatTest()
  {
    AppDataFormatter adf = new AppDataFormatter();

    adf.setContinuousFormatString(".00");

    final String expectedString = "3.50";

    Assert.assertEquals(expectedString, adf.format(3.5f));
    Assert.assertEquals(expectedString, adf.format(3.5));
  }

  @Test
  public void discreteFormatTest()
  {
    AppDataFormatter adf = new AppDataFormatter();

    adf.setDiscreteFormatString(".00");

    final String expectedString = "1.00";

    Assert.assertEquals(expectedString, adf.format((byte) 1));
    Assert.assertEquals(expectedString, adf.format((short) 1));
    Assert.assertEquals(expectedString, adf.format(1));
    Assert.assertEquals(expectedString, adf.format((long) 1));
  }

  @Test
  public void testObject()
  {
    AppDataFormatter adf = new AppDataFormatter();

    final String testString = "Hello World";

    Assert.assertEquals(testString, adf.format(testString));
  }

  @Test
  public void testDefaults()
  {
    AppDataFormatter adf = new AppDataFormatter();

    final String discreteString = "1";

    Assert.assertEquals(discreteString, adf.format((byte) 1));
    Assert.assertEquals(discreteString, adf.format((short) 1));
    Assert.assertEquals(discreteString, adf.format(1));
    Assert.assertEquals(discreteString, adf.format((long) 1));

    final String continuousString = "1.0";

    Assert.assertEquals(continuousString, adf.format((float) 1));
    Assert.assertEquals(continuousString, adf.format((double) 1));
  }
}
