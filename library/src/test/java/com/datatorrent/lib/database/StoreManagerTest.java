package com.datatorrent.lib.database;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import junit.framework.Assert;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

/**
 * Tests for {@link StoreManager}
 */
public class StoreManagerTest
{
  private static class DummyBackupStore implements Store.Backup
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
    public Map<Object, Object> fetchStartupData()
    {
      return Maps.filterKeys(backupMap, new Predicate<Object>()
      {
        @Override
        public boolean apply(@Nullable Object key)
        {
          return ((Integer) key).intValue() <= 5;
        }
      });
    }

    @Override
    public Object getValueFor(Object key)
    {
      return backupMap.get(key);
    }

    @Override
    public Map<Object, Object> bulkGet(final Set<Object> keys)
    {
      return Maps.filterEntries(backupMap, new Predicate<Map.Entry<Object, Object>>()
      {
        @Override
        public boolean apply(@Nullable Map.Entry<Object, Object> entry)
        {
          return keys.contains(entry.getKey());
        }
      });
    }

    @Override
    public void shutdownStore()
    {
      //Do nothing
    }
  }

  @Test
  public void testStoreManager()
  {
    Store.Primary primary = new CacheStore(new CacheProperties());
    StoreManager manager = new StoreManager(primary, new DummyBackupStore());
    manager.initialize(null);

    Assert.assertEquals("manager initialization- value", "one", primary.getValueFor(1));
    Assert.assertEquals("manager initializaton- total", 5, primary.getKeys().size());

    Assert.assertEquals("backup hit", "six", manager.get(6));
    Assert.assertEquals("primary updated- total", 6, primary.getKeys().size());
  }
}
