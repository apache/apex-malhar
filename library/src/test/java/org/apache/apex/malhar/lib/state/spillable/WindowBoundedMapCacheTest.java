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
package org.apache.apex.malhar.lib.state.spillable;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

public class WindowBoundedMapCacheTest
{
  @Test
  public void simplePutGetTest()
  {
    WindowBoundedMapCache<String, String> cache = new WindowBoundedMapCache<>();

    long windowId = 0L;

    windowId++;

    cache.put("1", "a");
    Assert.assertEquals("a", cache.get("1"));

    cache.endWindow();

    windowId++;

    Assert.assertEquals("a", cache.get("1"));

    cache.endWindow();
  }

  @Test
  public void getChangedGetRemovedTest()
  {
    WindowBoundedMapCache<String, String> cache = new WindowBoundedMapCache<>();

    long windowId = 0L;

    windowId++;

    cache.put("1", "a");
    cache.put("2", "b");

    Assert.assertEquals(Sets.newHashSet("1", "2"), cache.getChangedKeys());
    Assert.assertEquals(Sets.newHashSet(), cache.getRemovedKeys());

    cache.endWindow();

    windowId++;

    cache.remove("1");

    Assert.assertEquals(Sets.newHashSet(), cache.getChangedKeys());
    Assert.assertEquals(Sets.newHashSet("1"), cache.getRemovedKeys());

    Assert.assertEquals(null, cache.get("1"));
    Assert.assertEquals("b", cache.get("2"));

    cache.endWindow();

    windowId++;

    Assert.assertEquals(Sets.newHashSet(), cache.getChangedKeys());
    Assert.assertEquals(Sets.newHashSet(), cache.getRemovedKeys());

    cache.endWindow();
  }

  @Test
  public void expirationTest() throws Exception
  {
    WindowBoundedMapCache<String, String> cache = new WindowBoundedMapCache<>(2);

    long windowId = 0L;

    windowId++;

    cache.put("1", "a");
    Thread.sleep(1L);
    cache.put("2", "b");
    Thread.sleep(1L);
    cache.put("3", "c");

    Assert.assertEquals(Sets.newHashSet("1", "2", "3"), cache.getChangedKeys());

    cache.endWindow();

    windowId++;

    Assert.assertEquals(null, cache.get("1"));
    Assert.assertEquals("b", cache.get("2"));
    Assert.assertEquals("c", cache.get("3"));

    Assert.assertEquals(Sets.newHashSet(), cache.getChangedKeys());
    Assert.assertEquals(Sets.newHashSet(), cache.getRemovedKeys());

    cache.endWindow();
  }
}
