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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.util.KeyValPair;

/**
 * Unit test for TopNByKey accumulation
 */
public class TopNByKeyTest
{
  @Test
  public void TopNByKeyTest() throws Exception
  {
    TopNByKey<String, Integer> topNByKey = new TopNByKey<>();
    topNByKey.setN(3);
    Map<String, Integer> accu = topNByKey.defaultAccumulatedValue();

    Assert.assertEquals(0, accu.size());

    accu = topNByKey.accumulate(accu, new KeyValPair<String, Integer>("1", 1));
    accu = topNByKey.accumulate(accu, new KeyValPair<String, Integer>("3", 3));

    List<KeyValPair<String, Integer>> result1 = new ArrayList<>();

    result1.add(new KeyValPair<String, Integer>("3", 3));
    result1.add(new KeyValPair<String, Integer>("1", 1));

    Assert.assertEquals(result1, topNByKey.getOutput(accu));

    accu = topNByKey.accumulate(accu, new KeyValPair<String, Integer>("2", 2));

    List<KeyValPair<String, Integer>> result2 = new ArrayList<>();

    result2.add(new KeyValPair<String, Integer>("3", 3));
    result2.add(new KeyValPair<String, Integer>("2", 2));
    result2.add(new KeyValPair<String, Integer>("1", 1));

    Assert.assertEquals(result2, topNByKey.getOutput(accu));

    accu = topNByKey.accumulate(accu, new KeyValPair<String, Integer>("5", 5));
    accu = topNByKey.accumulate(accu, new KeyValPair<String, Integer>("4", 4));

    List<KeyValPair<String, Integer>> result3 = new ArrayList<>();

    result3.add(new KeyValPair<String, Integer>("5", 5));
    result3.add(new KeyValPair<String, Integer>("4", 4));
    result3.add(new KeyValPair<String, Integer>("3", 3));

    Assert.assertEquals(result3, topNByKey.getOutput(accu));
  }
}
