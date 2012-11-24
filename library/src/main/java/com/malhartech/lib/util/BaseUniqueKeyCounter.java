/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.HashMap;

/**
 * Count unique occurances of keys within a window<p>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator processes > 110 million tuples/sec. Only one tuple per unique key is emitted on end of window, so this operator is not bound by outbound throughput<br>
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class BaseUniqueKeyCounter<K> extends BaseKeyOperator<K>
{
  /**
   * Reference counts each tuple
   * @param tuple
   */
  public void processTuple(K tuple)
  {
    MutableInteger i = map.get(tuple);
    if (i == null) {
      i = new MutableInteger(0);
      map.put(cloneKey(tuple), i);
    }
    i.value++;
  }

  /**
   * Bucket counting mechanism.
   * Since we clear the bucket at the beginning of the window, we make this object transient.
   */
  protected transient HashMap<K, MutableInteger> map = new HashMap<K, MutableInteger>();

  /**
   * Clears cache/hash
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    map.clear();
  }
}
