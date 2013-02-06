/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.HashMap;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Count unique occurrences of keys within a window
 *
 * @param <K>
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class BaseUniqueKeyCounter<K> extends BaseKeyOperator<K>
{
  /**
   * Reference counts each tuple
   *
   * @param tuple
   */
  public void processTuple(K tuple)
  {
    MutableInt i = map.get(tuple);
    if (i == null) {
      i = new MutableInt(0);
      map.put(cloneKey(tuple), i);
    }
    i.increment();
  }

  /**
   * Bucket counting mechanism.
   * Since we clear the bucket at the beginning of the window, we make this object transient.
   */
  protected transient HashMap<K, MutableInt> map = new HashMap<K, MutableInt>();

  /**
   * Clears cache/hash
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    map.clear();
  }

}
