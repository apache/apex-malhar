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
package com.datatorrent.lib.appdata.datastructs;

import java.util.Map;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.util.TestUtils;

public class DimensionalTableTest
{
  @Test
  public void serializabilityTest() throws Exception
  {
    DimensionalTable<Integer> table = createTestTable();

    TestUtils.clone(new Kryo(), table);
  }

  @Test
  public void serializationRestorationTest() throws Exception
  {
    DimensionalTable<Integer> table = createTestTable();
    int size = table.size();

    table = TestUtils.clone(new Kryo(), table);
    Assert.assertEquals(size, table.size());
  }

  @Test
  public void getDataPointTest()
  {
    DimensionalTable<Integer> table = createTestTable();

    Integer point = table.getDataPoint(Lists.newArrayList("google", "taco bell", "Ukraine"));
    Assert.assertEquals((Integer) 6, point);

    Map<String, String> selectionValues = Maps.newHashMap();
    selectionValues.put("publisher", "amazon");
    selectionValues.put("advertiser", "burger king");
    selectionValues.put("location", "Czech");

    point = table.getDataPoint(selectionValues);
    Assert.assertEquals((Integer) 7, point);
  }

  @Test
  public void getDataPointsTest()
  {
    DimensionalTable<Integer> table = createTestTable();

    //Select on publisher
    Set<Integer> expectedDataPoints = Sets.newHashSet(3, 4, 5);
    Map<String, String> selectionKeys = Maps.newHashMap();
    selectionKeys.put("publisher", "twitter");

    Set<Integer> dataPoints = Sets.newHashSet(table.getDataPoints(selectionKeys));

    Assert.assertEquals(expectedDataPoints, dataPoints);

    //Select on advertiser
    expectedDataPoints = Sets.newHashSet(8, 9);
    selectionKeys.clear();
    selectionKeys.put("advertiser", "microsoft");

    dataPoints = Sets.newHashSet(table.getDataPoints(selectionKeys));

    Assert.assertEquals(expectedDataPoints, dataPoints);

    //Select on location
    expectedDataPoints = Sets.newHashSet(2, 10);
    selectionKeys.clear();
    selectionKeys.put("location", "NY");

    dataPoints = Sets.newHashSet(table.getDataPoints(selectionKeys));

    Assert.assertEquals(expectedDataPoints, dataPoints);
  }

  @Test
  public void duplicateAppendTest()
  {
    DimensionalTable<Integer> table = new DimensionalTable<Integer>(Lists.newArrayList("publisher",
                                                                                       "advertiser",
                                                                                       "location"));

    table.appendRow(1, "google", "starbucks", "CA");
    table.appendRow(2, "google", "starbucks", "CA");

    Assert.assertEquals(1, table.dataColumn.size());
    Assert.assertEquals(1, table.dimensionColumns.get(0).size());

    final Integer expectedPoint = 2;

    Map<String, String> row = Maps.newHashMap();
    row.put("publisher", "google");
    row.put("advertiser", "starbucks");
    row.put("location", "CA");

    table.appendRow(expectedPoint, row);

    Integer point = table.getDataPoint(row);
    Assert.assertEquals(expectedPoint, point);
  }

  @Test
  public void mapAppendTest()
  {
    DimensionalTable<Integer> table = createTestTable();

    final Integer expectedPoint = 1000;

    Map<String, String> row = Maps.newHashMap();
    row.put("publisher", "twitter");
    row.put("advertiser", "best buy");
    row.put("location", "san diego");

    table.appendRow(expectedPoint, row);

    Integer point = table.getDataPoint(row);
    Assert.assertEquals(expectedPoint, point);
  }

  private DimensionalTable<Integer> createTestTable()
  {
    DimensionalTable<Integer> table = new DimensionalTable<Integer>(Lists.newArrayList("publisher",
                                                                                       "advertiser",
                                                                                       "location"));

    table.appendRow(1, "google", "starbucks", "CA");
    table.appendRow(2, "amazon", "walmart", "NY");
    table.appendRow(3, "twitter", "safeway", "NV");
    table.appendRow(4, "twitter", "khol's", "AK");
    table.appendRow(5, "twitter", "starbucks", "Russia");
    table.appendRow(6, "google", "taco bell", "Ukraine");
    table.appendRow(7, "amazon", "burger king", "Czech");
    table.appendRow(8, "google", "microsoft", "Hungary");
    table.appendRow(9, "amazon", "microsoft", "Hungary");
    table.appendRow(10, "amazon", "starbucks", "NY");

    return table;
  }
}
