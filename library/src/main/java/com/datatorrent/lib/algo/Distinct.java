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
package com.datatorrent.lib.algo;

/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
import java.util.HashMap;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.BaseKeyOperator;

/**
 *
 * Computes and emits distinct tuples of type K (i.e drops duplicates) at end of window<p>
 * <br>
 * This module is same as a "FirstOf" metric on any key, val pair
 * Even though this module produces continuous tuples, at end of window all data is flushed. Thus the data set is windowed
 * and no history is kept of previous windows<br>
 * <br>
 * <b>StateFull : Yes, </b> tuple are compare across application window(s). <br>
 * <b>Partitions : Yes, </b> distinct output is unified by same operator. <br>
 * <br>
 * <b>Ports</b><br>
 * <b>data</b>: Input data port expects K<br>
 * <b>distinct</b>: Output data port, emits K<br>
 * <br>
 * <b>Properties</b>: None<br>
 * <br>
 *
 * @since 0.3.3
 */
public class Distinct<K> extends BaseKeyOperator<K> implements Unifier<K>
{
  /**
   * Distinct key map.
   */
  protected HashMap<K, Object> map = new HashMap<K, Object>();
  
  /**
   * Input port.
   */
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    /**
     * Process HashMap<K,V> tuple on input port data, and emits if match not found. Updates the cache
     * with new key,val pair
     */
    @Override
    public void process(K tuple)
    {
      if (!map.containsKey(tuple)) {
        distinct.emit(cloneKey(tuple));
        map.put(cloneKey(tuple), null);
      }
    }
  };
  
  /**
   *  Output for distinct values. </b>
   */
  @OutputPortFieldAnnotation(name = "distinct")
  public final transient DefaultOutputPort<K> distinct = new DefaultOutputPort<K>()
  {
    @Override
    public Unifier<K> getUnifier()
    {
      return new Distinct<K>();
    }
  };

  /**
   * Clears the cache/hash
   */
  @Override
  public void endWindow()
  {
    map.clear();
  }

  @Override
  public void process(K tuple)
  {
    if (!map.containsKey(tuple)) {
      distinct.emit(cloneKey(tuple));
      map.put(cloneKey(tuple), null);
    }
  }
}
