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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.netlet.util.Slice;

public class SerdeCollectionSliceTest
{
  @Test
  public void testSerdeList()
  {
    SerdeCollectionSlice<String, List<String>> serdeList =
        new SerdeCollectionSlice<>(new SerdeStringSlice(), (Class<List<String>>)(Class)ArrayList.class);

    List<String> stringList = Lists.newArrayList("a", "b", "c");

    Slice slice = serdeList.serialize(stringList);

    List<String> deserializedList = serdeList.deserialize(slice);

    Assert.assertEquals(stringList, deserializedList);
  }

  @Test
  public void testSerdeSet()
  {
    SerdeCollectionSlice<String, Set<String>> serdeSet =
        new SerdeCollectionSlice<>(new SerdeStringSlice(), (Class<Set<String>>)(Class)HashSet.class);

    Set<String> stringList = Sets.newHashSet("a", "b", "c");

    Slice slice = serdeSet.serialize(stringList);

    Set<String> deserializedSet = serdeSet.deserialize(slice);

    Assert.assertEquals(stringList, deserializedSet);
  }
}
