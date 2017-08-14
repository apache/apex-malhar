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
package org.apache.apex.malhar.lib.multiwindow;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.commons.lang.ObjectUtils.Null;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * A unit test to test SortedMovingWindow operator can either:
 * 1. sort simple comparable tuples
 * 2. sort tuples by given order (Comparator)
 * 3. group tuples into different category and sort the category by a given order
 *
 */
public class SortedMovingWindowTest
{

  /**
   * Test sorting simple comparable tuples within the sliding window
   */
  @Test
  public void testSortingSimpleNumberTuple()
  {
    SortedMovingWindow<Integer, Null> smw = new SortedMovingWindow<Integer, Null>();
    CollectorTestSink<Object> testSink = new CollectorTestSink<Object>();
    smw.outputPort.setSink(testSink);
    smw.setup(null);

    smw.setWindowSize(2);
    // The incoming 6 integer tuples are disordered among 4 windows
    emitObjects(smw, new Integer[][]{{1,3}, {2,5}, {4}, {6}});
    smw.beginWindow(4);
    smw.endWindow();
    smw.beginWindow(5);
    smw.endWindow();

    // The outcome is sorted
    Assert.assertEquals(Lists.newArrayList(1, 2, 3, 4, 5, 6), testSink.collectedTuples);

  }

  /**
   * Given sorting key, sorting function, test sorting the map tuples within the sliding window
   */
  @Test
  public void testSortingMapTupleWithoutKey()
  {

    SortedMovingWindow<Map<String, Integer>, Null> smw = new SortedMovingWindow<Map<String, Integer>, Null>();

    final String[] keys = {"number"};

    smw.setComparator(new Comparator<Map<String, Integer>>()
    {
      @Override
      public int compare(Map<String, Integer> o1, Map<String, Integer> o2)
      {
        // order the map by the value of key "number"
        return o1.get(keys[0]) - o2.get(keys[0]);
      }
    });
    CollectorTestSink<Object> testSink = new CollectorTestSink<Object>();
    smw.outputPort.setSink(testSink);
    smw.setup(null);
    smw.setWindowSize(2);

    // The incoming 6 simple map tuples are disordered among 4 windows
    emitObjects(smw, new Map[][]{createHashMapTuples(keys, new Integer[][]{{1}, {3}}),
        createHashMapTuples(keys, new Integer[][]{{2}, {5}}),
        createHashMapTuples(keys, new Integer[][]{{4}}), createHashMapTuples(keys, new Integer[][]{{6}})});
    smw.beginWindow(4);
    smw.endWindow();
    smw.beginWindow(5);
    smw.endWindow();

    // The outcome is ordered by the value of the key "number"
    Assert.assertEquals(Arrays.asList(createHashMapTuples(keys, new Integer[][]{{1}, {2}, {3}, {4}, {5}, {6}})),
        testSink.collectedTuples);
  }


  /**
   * Given grouping key, sorting key and sorting function, test sorting the map tuples within the sliding window
   */
  @Test
  public void testSortingMapTupleWithKey()
  {

    SortedMovingWindow<Map<String, Object>, String> smw = new SortedMovingWindow<Map<String, Object>, String>();

    final String[] keys = {"name", "number"};

    smw.setComparator(new Comparator<Map<String, Object>>()
    {
      @Override
      public int compare(Map<String, Object> o1, Map<String, Object> o2)
      {
        // order by key "number"
        return (Integer)o1.get(keys[1]) - (Integer)o2.get(keys[1]);
      }
    });

    smw.setFunction(new Function<Map<String,Object>, String>()
    {
      @Override
      public String apply(Map<String, Object> input)
      {
        // order tuple with same key "name"
        return (String)input.get(keys[0]);
      }
    });
    CollectorTestSink<Object> testSink = new CollectorTestSink<Object>();
    smw.outputPort.setSink(testSink);
    smw.setup(null);
    smw.setWindowSize(2);

    // The incoming 9 complex map tuples are disordered with same name among 4 windows
    emitObjects(smw, new Map[][]{createHashMapTuples(keys, new Object[][]{{"bob", 1}, {"jim", 1}}),
        createHashMapTuples(keys, new Object[][]{{"jim", 2}, {"bob", 3}}),
        createHashMapTuples(keys, new Object[][]{{"bob", 2}, {"jim", 4}}),
        createHashMapTuples(keys, new Object[][]{{"bob", 5}, {"jim", 3}, {"bob", 4}})});
    smw.beginWindow(4);
    smw.endWindow();
    smw.beginWindow(5);
    smw.endWindow();

    // All tuples with same "name" are sorted by key "number"
    Assert.assertEquals(Arrays.asList(createHashMapTuples(keys,
        new Object[][]{{"bob", 1}, {"jim", 1}, {"jim", 2}, {"bob", 2}, {"bob", 3}, {"jim", 3}, {"jim", 4}, {"bob", 4}, {"bob", 5}})), testSink.collectedTuples);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void emitObjects(SortedMovingWindow win, Object[][] obj)
  {
    for (int i = 0; i < obj.length; i++) {
      win.beginWindow(i);
      for (int j = 0; j < obj[i].length; j++) {
        win.data.process(obj[i][j]);
      }
      win.endWindow();
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Map[] createHashMapTuples(String[] cols, Object[][] values)
  {

    HashMap[] maps = new HashMap[values.length];
    int index = -1;
    for (Object[] vs : values) {
      maps[++index] = new HashMap<String, Object>();
      int colIndex = 0;
      for (Object value : vs) {
        maps[index].put(cols[colIndex++], value);
      }
    }

    return maps;
  }

}
