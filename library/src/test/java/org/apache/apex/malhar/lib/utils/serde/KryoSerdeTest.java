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

import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.collect.Lists;

import com.datatorrent.netlet.util.Slice;

/**
 * SerdeKryoSlice unit tests
 */
public class KryoSerdeTest
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
    KryoSerde<ArrayList> serdeList = new KryoSerde<>(ArrayList.class);

    ArrayList<String> stringList = Lists.newArrayList("a", "b", "c");
    SerializationBuffer buffer = new DefaultSerializationBuffer(new WindowedBlockStream());
    serdeList.serialize(stringList, buffer);
    Slice slice = buffer.toSlice();
    List<String> deserializedList = serdeList.deserialize(slice.buffer, new MutableInt(slice.offset), slice.length);
    Assert.assertEquals(stringList, deserializedList);
  }

  @Test
  public void pojoTest()
  {
    KryoSerde<TestPojo> serdePojo = new KryoSerde<>();
    TestPojo pojo = new TestPojo(345, "xyz");
    SerializationBuffer buffer = new DefaultSerializationBuffer(new WindowedBlockStream());
    serdePojo.serialize(pojo, buffer);
    Slice slice = buffer.toSlice();
    TestPojo deserializedPojo = serdePojo.deserialize(slice.buffer, new MutableInt(slice.offset), slice.length);
    Assert.assertEquals(pojo, deserializedPojo);
  }
}
