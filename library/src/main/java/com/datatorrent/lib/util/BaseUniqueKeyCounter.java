/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.util;

import java.util.HashMap;
import org.apache.commons.lang.mutable.MutableInt;

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
  protected HashMap<K, MutableInt> map = new HashMap<K, MutableInt>();
}
