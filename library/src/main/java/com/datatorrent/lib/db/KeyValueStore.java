/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.db;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @since 0.9.3
 */
public interface KeyValueStore
{
  public Object get(Object key);
  public List<Object> getAll(List<Object> keys);

  public void put(Object key, Object value);
  public void putAll(Map<Object, Object> m);

  public void remove(Object key);
}
