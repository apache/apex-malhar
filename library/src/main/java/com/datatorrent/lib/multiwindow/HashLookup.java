/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.multiwindow;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.BaseFilteredKeyValueOperator;
import java.util.HashMap;
import javax.validation.constraints.Min;

/**
 * <b>Not certified yet. Do not use</b>
 * A hash lookup class, maintains hashes for as many previous windows as users asks and returns the oldest matching lookup on query, and clears the oldest hash
 * on beginWindow<p>
 * This module is a pass through<br>
 * <br>
 * <b>Ports</b>: None<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple<br>
 * <b>filterBy</b>: List of keys to filter on<br>
 * <br>
 * <b>n</b>: Number of windows to keep the hash for. Default value is 2<br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for HashLookup&lt;K,V&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>TBD</b></td><td>TBD</td><td>TBD</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table</b>: Not relevant as it is a HashMap&lt;K,v&gt; lookup<br>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
 */

public class HashLookup<K, V> extends BaseFilteredKeyValueOperator<K,V>
{
  HashMap<K,V> [] maps = null;
  HashMap<K,V> currentmap = null;
  int current = 0;

  @Min(1)
  int n = 2;

  /**
   * getter function for n (depth)
   * @return n
   */
  @Min(1)
  public int getN()
  {
    return n;
  }

  /**
   * setter function for n
   * @param i
   */
  void setN(int i)
  {
    n = i;
  }

  @Override
  public void setup(OperatorContext context)
  {
    maps = new HashMap[n];
    for (int i = 0; i < n; i++) {
      maps[i] = new HashMap<K,V>();
    }
    current = 0;
  }


  public V put(K key, V val)
  {
    return currentmap.put(cloneKey(key),cloneValue(val));
  }

  /**
   * A multi-window get
   * @param key to get the value of
   * @return V
   */
  public V get(K key)
  {
    V ret = null;
    for (int i = current; i < n; i++) {
      ret = maps[i].get(key);
      if (ret != null) {
        break;
      }
    }
    if (ret == null) {
      for (int i = 0; i < current; i++) {
        ret = maps[i].get(key);
        if (ret != null) {
          break;
        }
      }
    }
    return ret;
  }


  /**
   * A multi-window containsKey. Looks at current map and goes back one at a time
   * @param key
   * @return true if key is there
   */
  public boolean containsKey(K key)
  {
    boolean ret = false;
    for (int i = current; i < n; i++) {
      ret = maps[i].containsKey(key);
      if (ret) {
        break;
      }
    }
    if (!ret) {
      for (int i = 0; i < current; i++) {
        ret = maps[i].containsKey(key);
        if (ret) {
          break;
        }
      }
    }
    return ret;
  }


  public void cleanUp(HashMap<K,V> map)
  {
    map.clear();
  }

  /**
   * Resets the matched variable
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    current++;
    if (current >= n) {
      current = 0;
    }
    currentmap = maps[current];
    cleanUp(currentmap);
  }
}
