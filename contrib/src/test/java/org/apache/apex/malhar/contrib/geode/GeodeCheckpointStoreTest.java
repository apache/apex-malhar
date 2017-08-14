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
package org.apache.apex.malhar.contrib.geode;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.google.common.collect.Maps;

public class GeodeCheckpointStoreTest
{

  static String LOCATOR_HOST = "localhost:10334";
  static final String REGION_NAME = "apex-checkpoint-region";

  static final String KEY1 = "key1";
  static final String KEY2 = "key2";
  static final String KEY3 = "key3";

  static GeodeCheckpointStore store;

  @Before
  public void setUp() throws Exception
  {
    if (System.getProperty("dev.locator.connection") != null) {
      LOCATOR_HOST = System.getProperty("dev.locator.connection");
    }

    store = new GeodeCheckpointStore(LOCATOR_HOST);
    store.setTableName(REGION_NAME);
    store.connect();
  }

  @Test
  public void testSave() throws IOException
  {
    Map<Integer, String> data = Maps.newHashMap();
    data.put(1, "one");
    data.put(2, "two");
    data.put(3, "three");
    store.put(KEY1, data);
    @SuppressWarnings("unchecked")
    Map<Integer, String> decoded = (Map<Integer, String>)store.get(KEY1);
    Assert.assertEquals("dataOf1", data, decoded);
  }

  @Test
  public void testLoad() throws IOException
  {
    Map<Integer, String> dataOf1 = Maps.newHashMap();
    dataOf1.put(1, "one");
    dataOf1.put(2, "two");
    dataOf1.put(3, "three");

    Map<Integer, String> dataOf2 = Maps.newHashMap();
    dataOf2.put(4, "four");
    dataOf2.put(5, "five");
    dataOf2.put(6, "six");

    store.put(KEY1, dataOf1);
    store.put(KEY2, dataOf2);
    @SuppressWarnings("unchecked")
    Map<Integer, String> decoded1 = (Map<Integer, String>)store.get(KEY1);

    @SuppressWarnings("unchecked")
    Map<Integer, String> decoded2 = (Map<Integer, String>)store.get(KEY2);
    Assert.assertEquals("data of 1", dataOf1, decoded1);
    Assert.assertEquals("data of 2", dataOf2, decoded2);
  }

  @Test
  public void testDelete() throws IOException, FunctionDomainException, TypeMismatchException, NameResolutionException,
    QueryInvocationTargetException
  {
    testLoad();

    store.remove(KEY1);
    Assert.assertTrue("operator 2 window 1", (store.get(KEY2) != null));
    Assert.assertFalse("operator 1 window 1", (store.get(KEY1) != null));
  }

  //@Test
  public void testGetWindowIds() throws IOException
  {
    store.disconnect();
    final String REGION_NAME = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date());
    store.setTableName(REGION_NAME);
    store.connect();
    Map<Integer, String> obj = Maps.newHashMap();
    obj.put(1, "one");
    obj.put(2, "two");
    obj.put(3, "three");

    List<String> op1WindowIds = new ArrayList<>();
    op1WindowIds.add("111");
    op1WindowIds.add("112");
    op1WindowIds.add("113");
    for (String l : op1WindowIds) {
      store.put(l, obj);
    }

    List<String> op2WindowIds = new ArrayList<>();
    op2WindowIds.add("211");
    op2WindowIds.add("212");
    for (String l : op2WindowIds) {
      store.put(l, obj);
    }

    List<String> op1WinIds = store.getKeys(1);
    List<String> op2WinIds = store.getKeys(2);

    Assert.assertEquals(op1WindowIds.size(), op1WinIds.size());
    Assert.assertEquals(op2WindowIds.size(), op2WinIds.size());
    Assert.assertTrue(op1WindowIds.containsAll(op1WinIds));
    Assert.assertTrue(op2WindowIds.containsAll(op2WinIds));
  }

  @After
  public void tearDown() throws Exception
  {
    store.disconnect();
  }

}
