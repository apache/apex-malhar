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

import com.google.common.collect.Lists;

import com.datatorrent.netlet.util.Slice;

/**
 * SerdeKryoSlice unit tests
 */
public class SerdeKryoSliceTest
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
    SerdeKryoSlice<ArrayList> serdeList = new SerdeKryoSlice<>(ArrayList.class);

    ArrayList<String> stringList = Lists.newArrayList("a", "b", "c");
    Slice slice = serdeList.serialize(stringList);
    List<String> deserializedList = serdeList.deserialize(slice);
    Assert.assertEquals(stringList, deserializedList);
  }

  @Test
  public void pojoTest()
  {
    SerdeKryoSlice<TestPojo> serdePojo = new SerdeKryoSlice<>();
    TestPojo pojo = new TestPojo(345, "xyz");
    Slice slice = serdePojo.serialize(pojo);
    TestPojo deserializedPojo = serdePojo.deserialize(slice);
    Assert.assertEquals(pojo, deserializedPojo);
  }
}
