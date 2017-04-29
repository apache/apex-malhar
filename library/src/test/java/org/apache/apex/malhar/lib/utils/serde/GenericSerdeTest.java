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
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.Window;

import com.esotericsoftware.kryo.io.Input;
import com.google.common.collect.Lists;

import com.datatorrent.netlet.util.Slice;

/**
 * SerdeKryoSlice unit tests
 */
public class GenericSerdeTest
{
  public static class TestPojo
  {
    private TestPojo()
    {
    }

    public TestPojo(int intValue, String stringValue)
    {
      this.intValue = intValue;
      this.stringValue = stringValue;
    }

    @Override
    public boolean equals(Object other)
    {
      TestPojo o = (TestPojo)other;
      return intValue == o.intValue && stringValue.equals(o.stringValue);
    }

    int intValue;
    String stringValue;
  }

  @Test
  public void stringListTest()
  {
    GenericSerde<ArrayList> serdeList = new GenericSerde<>(ArrayList.class);

    ArrayList<String> stringList = Lists.newArrayList("a", "b", "c");
    SerializationBuffer buffer = new SerializationBuffer(new WindowedBlockStream());
    serdeList.serialize(stringList, buffer);
    Slice slice = buffer.toSlice();
    List<String> deserializedList = serdeList.deserialize(new Input(slice.buffer, slice.offset, slice.length));
    Assert.assertEquals(stringList, deserializedList);
  }


  @Test
  public void pojoTest()
  {
    GenericSerde<TestPojo> serdePojo = new GenericSerde<>();
    TestPojo pojo = new TestPojo(345, "xyz");
    SerializationBuffer buffer = new SerializationBuffer(new WindowedBlockStream());
    serdePojo.serialize(pojo, buffer);
    Slice slice = buffer.toSlice();
    TestPojo deserializedPojo = serdePojo.deserialize(new Input(slice.buffer, slice.offset, slice.length));
    Assert.assertEquals(pojo, deserializedPojo);
  }

  @Test
  public void timeWindowSerdeTest()
  {
    GenericSerde<Window.TimeWindow>[] serdes = new GenericSerde[] {new GenericSerde<>(Window.TimeWindow.class), GenericSerde.DEFAULT};
    for (GenericSerde<Window.TimeWindow> serde : serdes) {
      Window.TimeWindow pojo = new Window.TimeWindow(System.currentTimeMillis(), 1000);
      SerializationBuffer buffer = new SerializationBuffer(new WindowedBlockStream());
      serde.serialize(pojo, buffer);
      Slice slice = buffer.toSlice();
      Window.TimeWindow deserializedPojo = serde.deserialize(new Input(slice.buffer, slice.offset, slice.length));
      Assert.assertEquals(pojo, deserializedPojo);
    }
  }
}
