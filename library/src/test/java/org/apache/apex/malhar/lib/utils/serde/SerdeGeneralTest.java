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
package org.apache.apex.malhar.lib.utils.serde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.netlet.util.Slice;

public class SerdeGeneralTest
{
  protected final int charNum = 62;
  protected String[] testData = null;
  protected final Random random = new Random();

  @Before
  public void generateTestData()
  {
    int size = random.nextInt(10000) + 1;
    testData = new String[size];
    for (int i = 0; i < size; ++i) {
      char[] chars = new char[random.nextInt(10000) + 1];
      for (int j = 0; j < chars.length; ++j) {
        chars[j] = getRandomChar();
      }

      testData[i] = new String(chars);
    }
  }

  protected char getRandomChar()
  {
    int value = random.nextInt(62);
    if (value < 10) {
      return (char)(value + '0');
    } else if (value < 36) {
      return (char)(value + 'A');
    }
    return (char)(value + 'a');
  }

  @Test
  public void testSerdeSimpleString()
  {
    StringSerde serde = new StringSerde();
    SerializationBuffer buffer = new DefaultSerializationBuffer();
    String[] strs = new String[]{"a", "ab", "cde"};
    for (String str : strs) {
      serde.serialize(str, buffer);
      Slice slice = buffer.toSlice();
      String newStr = serde.deserialize(slice.buffer, new MutableInt(slice.offset), slice.length);
      Assert.assertEquals(newStr, str);
    }
  }

  @Test
  public void testSerdeString()
  {
    testSerde(testData, new StringSerde(), new StringSerdeVerifier());
  }

  @Test
  public void testSerdeArray()
  {
    testSerde(testData, ArraySerde.newSerde(new StringSerde(), String.class), new StringArraySerdeVerifier());
  }


  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testSerdeCollection()
  {
    CollectionSerde<String, List<String>> listSerde = new CollectionSerde<>(new StringSerde(), (Class)ArrayList.class);
    testSerde(testData, listSerde, new StringListSerdeVerifier());
  }


  public <T> void testSerde(String[] strs, Serde<T> serde, SerdeVerifier<T> verifier)
  {
    DefaultSerializationBuffer buffer = new DefaultSerializationBuffer(new WindowedBlockStream());

    for (int i = 0; i < 10; ++i) {
      buffer.beginWindow(i);
      verifier.verifySerde(strs, serde, buffer);
      buffer.endWindow();
      if (i % 3 == 0) {
        buffer.getOutputStream().resetUpToWindow(i);
      }
      if (i % 4 == 0) {
        buffer.reset();
      }
    }
    buffer.release();
  }

  public static interface SerdeVerifier<T>
  {
    public void verifySerde(String[] datas, Serde<T> serde, DefaultSerializationBuffer buffer);
  }

  public static class StringSerdeVerifier implements SerdeVerifier<String>
  {
    @Override
    public void verifySerde(String[] datas, Serde<String> serde, DefaultSerializationBuffer buffer)
    {
      for (String str : datas) {
        serde.serialize(str, buffer);
        Slice slice = buffer.toSlice();
        Assert.assertTrue("serialize failed, String: " + str, str.equals(serde.deserialize(slice.buffer, new MutableInt(slice.offset), slice.length)));
      }
    }
  }


  public static class StringArraySerdeVerifier implements SerdeVerifier<String[]>
  {
    @Override
    public void verifySerde(String[] datas, Serde<String[]> serde, DefaultSerializationBuffer buffer)
    {
      serde.serialize(datas, buffer);
      Slice slice = buffer.toSlice();
      String[] newStrs = serde.deserialize(slice.buffer, new MutableInt(slice.offset), slice.length);
      Assert.assertArrayEquals("serialize array failed.", datas, newStrs);
    }
  }

  public static class StringListSerdeVerifier implements SerdeVerifier<List<String>>
  {
    @Override
    public void verifySerde(String[] datas, Serde<List<String>> serdeList, DefaultSerializationBuffer buffer)
    {
      List<String> list = Arrays.asList(datas);

      serdeList.serialize(list, buffer);
      Slice slice = buffer.toSlice();
      List<String> newStrs = serdeList.deserialize(slice.buffer, new MutableInt(slice.offset), slice.length);
      Assert.assertArrayEquals("serialize list failed.", datas, newStrs.toArray(new String[0]));

      buffer.reset();
    }
  }

}
