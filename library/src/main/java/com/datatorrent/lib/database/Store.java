package com.datatorrent.lib.database;

import java.util.Map;
import java.util.Set;

/**
 * <br>A store which could be a memory store or a database store.</br>
 */
public interface Store
{
  Object getValueFor(Object key);

  Map<Object, Object> bulkGet(Set<Object> keys);

  void shutdownStore();

  /**
   * <br>A primary store should also provide setting the value for a key.</br>
   */
  public static interface Primary extends Store
  {
    void setValueFor(Object key, Object value);

    Set<Object> getKeys();

    void bulkSet(Map<Object, Object> data);
  }

  /**
   * <br>Backup store is queried when {@link Primary} doesn't contain a key.</br>
   * <br>It also provides data needed at startup.</br>
   */
  public static interface Backup extends Store
  {
    Map<Object, Object> fetchStartupData();
  }

}
