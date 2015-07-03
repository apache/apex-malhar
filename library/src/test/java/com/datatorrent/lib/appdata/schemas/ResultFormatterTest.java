/*
 * Copyright (c) 2015 DataTorrent, Inc.
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

import com.esotericsoftware.kryo.Kryo;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.util.TestUtils;

public class ResultFormatterTest
{
  @Test
  public void serializationTest() throws Exception
  {
    TestUtils.clone(new Kryo(), new ResultFormatter());
  }

  @Test
  public void continuousFormatTest()
  {
    ResultFormatter adf = new ResultFormatter();

    adf.setContinuousFormatString(".00");

    final String expectedString = "3.50";

    Assert.assertEquals(expectedString, adf.format(3.5f));
    Assert.assertEquals(expectedString, adf.format(3.5));
  }

  @Test
  public void discreteFormatTest()
  {
    ResultFormatter adf = new ResultFormatter();

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
    ResultFormatter adf = new ResultFormatter();

    final String testString = "Hello World";

    Assert.assertEquals(testString, adf.format(testString));
  }

  @Test
  public void testDefaults()
  {
    ResultFormatter adf = new ResultFormatter();

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
