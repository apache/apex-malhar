package com.datatorrent.lib.database;

import java.util.Map;

/**
 * <br>A store</br>
 */
public abstract class Store
{
  protected abstract Object getValueFor(Object key);

  protected abstract void shutdownStore();

  /**
   * <br>A primary store should also provide setting the value for a key.</br>
   */
  public static abstract class Primary extends Store
  {
    abstract void setValueFor(Object key, Object value);

    abstract void bulkSet(Map<Object, Object> data);
  }

  /**
   * <br>Backup store is queried when {@link Primary} doesn't contain a key.</br>
   * <br>It also provides data needed at startup.</br>
   */
  public static abstract class Backup extends Store
  {
    abstract Map<Object, Object> fetchStartupData();
  }

}
