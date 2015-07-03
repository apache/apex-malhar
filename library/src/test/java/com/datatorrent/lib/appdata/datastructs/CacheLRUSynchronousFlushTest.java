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

import java.util.List;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.appdata.datastructs.CacheLRUSynchronousFlush.CacheFlushListener;

public class CacheLRUSynchronousFlushTest
{
  @Test
  public void simplePutAndRemove()
  {
    final Integer key = 1;
    final Integer value = 1;

    CacheLRUSynchronousFlush<Integer, Integer> cache =
    new CacheLRUSynchronousFlush<Integer, Integer>(new NOPCacheFlushListener<Integer, Integer>());

    cache.put(key, value);

    Assert.assertEquals("The values should equal", value, cache.get(key));

    Integer removedVal = cache.remove(key);

    Assert.assertEquals("The removed value must equal the old value", value, removedVal);
    Assert.assertEquals("The retrieved value should be null.", null, cache.get(key));
  }

  @Test
  public void simpleFlushChanges()
  {
    final Integer key = 1;
    final Integer value = 1;
    final Integer value2 = 2;

    TestFlushListener<Integer, Integer> fl = new TestFlushListener<Integer, Integer>();
    CacheLRUSynchronousFlush<Integer, Integer> cache = new CacheLRUSynchronousFlush<Integer, Integer>(fl);

    cache.put(key, value);
    cache.flushChanges();

    Assert.assertEquals("Expecting one key", 1, fl.flushedKeys.size());
    Assert.assertEquals("Expecting one value", 1, fl.flushedValues.size());

    Assert.assertEquals("The keys must equal", key, fl.flushedKeys.get(0));
    Assert.assertEquals("The values must equal", value, fl.flushedValues.get(0));

    fl.flushedKeys.clear();
    fl.flushedValues.clear();

    cache.flushChanges();

    Assert.assertEquals("Expecting no keys", 0, fl.flushedKeys.size());
    Assert.assertEquals("Expecting no values", 0, fl.flushedValues.size());

    cache.put(1, value2);

    cache.flushChanges();

    Assert.assertEquals("Expecting one key", 1, fl.flushedKeys.size());
    Assert.assertEquals("Expecting one value", 1, fl.flushedValues.size());

    Assert.assertEquals("The keys must equal", key, fl.flushedKeys.get(0));
    Assert.assertEquals("The values must equal", value2, fl.flushedValues.get(0));
  }

  @Test
  public void simpleRemoveExcessTest()
  {
    final Integer key = 1;
    final Integer key2 = 2;
    final Integer value = 1;
    final Integer value2 = 2;

    int flushedSize = 1;

    TestFlushListener<Integer, Integer> fl = new TestFlushListener<Integer, Integer>();
    CacheLRUSynchronousFlush<Integer, Integer> cache = new CacheLRUSynchronousFlush<Integer, Integer>(flushedSize, fl);

    cache.put(key, value);

    try {
      Thread.sleep(10);
    }
    catch(InterruptedException ex) {
      throw new RuntimeException(ex);
    }

    cache.put(key2, value2);

    cache.removeExcess();

    Assert.assertEquals("Expecting one key", 1, fl.flushedKeys.size());
    Assert.assertEquals("Expecting one value", 1, fl.flushedValues.size());

    Assert.assertEquals("The keys must equal", key, fl.flushedKeys.get(0));
    Assert.assertEquals("The values must equal", value, fl.flushedValues.get(0));

    Assert.assertEquals("This value should no longer be in the cache", null, cache.get(key));
    Assert.assertEquals("This value should still be in the cache", value2, cache.get(key2));

    fl.flushedKeys.clear();
    fl.flushedValues.clear();

    cache.flushChanges();

    Assert.assertEquals("Expecting one key", 1, fl.flushedKeys.size());
    Assert.assertEquals("Expecting one value", 1, fl.flushedValues.size());

    Assert.assertEquals("The keys must equal", key2, fl.flushedKeys.get(0));
    Assert.assertEquals("The values must equal", value2, fl.flushedValues.get(0));
  }

  public class TestFlushListener<KEY, VALUE> implements CacheFlushListener<KEY, VALUE>
  {
    public List<KEY> flushedKeys = Lists.newArrayList();
    public List<VALUE> flushedValues = Lists.newArrayList();

    @Override
    public void flush(KEY key, VALUE value)
    {
      flushedKeys.add(key);
      flushedValues.add(value);
    }
  }
}
