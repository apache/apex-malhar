/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.algo;

import java.util.HashMap;

import org.apache.apex.malhar.lib.util.BaseKeyOperator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * This operator computes and emits distinct tuples of type K (i.e drops duplicates) at end of window.
 * <p>
 * Computes and emits distinct tuples of type K (i.e drops duplicates) at end of window
 * </p>
 * <p>
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
 * </p>
 *
 * @displayName Emit Distinct
 * @category Rules and Alerts
 * @tags filter, unique
 *
 * @since 0.3.3
 */

@OperatorAnnotation(partitionable = true)
public class Distinct<K> extends BaseKeyOperator<K> implements Unifier<K>
{
  /**
   * Distinct key map.
   */
  protected HashMap<K, Object> map = new HashMap<K, Object>();

  /**
   * The input port on which tuples are received.
   */
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>()
  {
    /**
     * Process HashMap<K,V> tuple on input port data, and emits if match not found. Updates the cache
     * with new key,val pair
     */
    @Override
    public void process(K tuple)
    {
      Distinct.this.process(tuple);
    }
  };

  /**
   *  The output port on which distinct values are emitted.
   */
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
