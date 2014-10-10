/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.db.cache;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Tests for {@link CacheManager}
 */
public class CacheManagerTest
{
  private static class DummyBackupStore implements CacheManager.Backup
  {

    private static final Map<Object, Object> backupMap = Maps.newHashMap();

    static {
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
    }

    @Override
    public Map<Object, Object> loadInitialData()
    {
      return Maps.filterKeys(backupMap, new Predicate<Object>()
      {
        @Override
        public boolean apply(@Nullable Object key)
        {
          return key != null && (Integer) key <= 5;
        }
      });
    }

    @Override
    public Object get(Object key)
    {
      return backupMap.get(key);
    }

    @Override
    public List<Object> getAll(final List<Object> keys)
    {
      List<Object> values = Lists.newArrayList();
      for (Object key : keys) {
        values.add(backupMap.get(key));
      }
      return values;
    }

    @Override
    public void put(Object key, Object value)
    {
      backupMap.put(key, value);
    }

    @Override
    public void putAll(Map<Object, Object> m)
    {
      throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void remove(Object key)
    {
      throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void connect() throws IOException
    {
    }

    @Override
    public void disconnect() throws IOException
    {
    }

    @Override
    public boolean connected()
    {
      return true;
    }
  }

  @Test
  public void testCacheManager() throws IOException
  {
    CacheManager manager = new CacheManager();
    manager.setBackup(new DummyBackupStore());
    manager.initialize();

    Assert.assertEquals("manager initialization- value", "one", manager.primary.get(1));
    Assert.assertEquals("manager initializaton- total", 5, manager.primary.getKeys().size());

    Assert.assertEquals("backup hit", "six", manager.get(6));
    Assert.assertEquals("primary updated- total", 6, manager.primary.getKeys().size());
  }
}
