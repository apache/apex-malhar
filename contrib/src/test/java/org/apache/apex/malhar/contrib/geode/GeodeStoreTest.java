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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.gemstone.gemfire.cache.query.SelectResults;

public class GeodeStoreTest
{

  private GeodeStore geodeStore;

  @Before
  public void setup() throws IOException
  {
    geodeStore = new GeodeStore();
    geodeStore.setLocatorHost("192.168.1.128");
    if (System.getProperty("dev.locator.connection") != null) {
      geodeStore.setLocatorHost(System.getProperty("dev.locator.connection"));
    }
    geodeStore.setLocatorPort(10334);
    geodeStore.setRegionName("operator");
    geodeStore.connect();
  }

  @After
  public void tearDown() throws IOException
  {
    geodeStore.disconnect();
  }

  @Test
  public void testputAllandget() throws Exception
  {

    Map<Object, Object> m = new HashMap<Object, Object>();
    m.put("test1_abc", "123");
    m.put("test1_def", "456");

    geodeStore.putAll(m);

    Assert.assertEquals("123", geodeStore.get("test1_abc"));
    Assert.assertEquals("456", geodeStore.get("test1_def"));
  }

  @Test
  public void testQuery() throws Exception
  {
    Map<Object, Object> m = new HashMap<Object, Object>();
    m.put("test2_abc", "123");
    m.put("test2_def", "456");
    geodeStore.putAll(m);
    String predicate = "Select key,value from /operator.entries where key like 'test2%'";
    SelectResults results = geodeStore.query(predicate);
    Assert.assertEquals(2, results.size());
  }

  @Test
  public void testputAllandgetAll() throws Exception
  {

    Map<Object, Object> m = new HashMap<Object, Object>();
    m.put("test3_abc", "123");
    m.put("test3_def", "456");
    geodeStore.putAll(m);

    List<Object> keys = new ArrayList<Object>();
    keys.add("test3_abc");
    keys.add("test3_def");
    Map<Object, Object> values = geodeStore.getAllMap(keys);

    Assert.assertEquals("123", values.get("test3_abc"));
    Assert.assertEquals("456", values.get("test3_def"));

  }

  @Test
  public void testputandget() throws Exception
  {
    geodeStore.put("test4_abc", "123");
    Assert.assertEquals("123", geodeStore.get("test4_abc"));
  }

}
