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
package org.apache.apex.malhar.lib.db.cache;

import java.io.IOException;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * Test fot {@link CacheStore}.
 */
public class CacheStoreTest
{
  @Test
  public void CacheStoreTest()
  {
    final Map<Object, Object> backupMap = Maps.newHashMap();

    backupMap.put(1, "one");
    backupMap.put(2, "two");
    backupMap.put(3, "three");
    backupMap.put(4, "four");
    backupMap.put(5, "five");
    backupMap.put(6, "six");
    backupMap.put(7, "seven");
    backupMap.put(8, "eight");
    backupMap.put(9, "nine");
    backupMap.put(10, "ten");

    CacheStore cs = new CacheStore();
    cs.setMaxCacheSize(5);
    try {
      cs.connect();
      cs.putAll(backupMap);
      Assert.assertEquals(5, cs.getKeys().size());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
