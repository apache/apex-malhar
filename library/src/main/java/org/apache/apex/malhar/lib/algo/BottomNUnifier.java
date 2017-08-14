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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.Min;

import org.apache.apex.malhar.lib.util.TopNSort;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;

/**
 * This unifier takes an input stream of key value pairs is ordered by key,
 * and the bottom N of the ordered tuples per key are emitted on port "bottom" at the end of window.
 * <p></p>
 * @displayName Bottom N Unifier
 * @category Algorithmic
 * @tags filter, rank, key value
 *
 * @since 0.3.3
 */
public class BottomNUnifier<K, V> implements Unifier<HashMap<K, ArrayList<V>>>
{
  /**
   * Merged tuples map.
   */
  protected HashMap<K, TopNSort<V>> kmap = new HashMap<K, TopNSort<V>>();

  /**
   * set n value.
   */
  @Min(1)
  int n = 1;

  /**
   * The output port on which the bottom N tuples per key are emitted.
   */
  public final transient DefaultOutputPort<HashMap<K, ArrayList<V>>> mergedport = new DefaultOutputPort<HashMap<K, ArrayList<V>>>();

  @Override
  public void beginWindow(long arg0)
  {
    // TODO Auto-generated method stub

  }


  @Override
  public void endWindow()
  {
    for (Map.Entry<K, TopNSort<V>> e : kmap.entrySet()) {
      HashMap<K, ArrayList<V>> tuple = new HashMap<K, ArrayList<V>>(1);
      tuple.put(e.getKey(), (ArrayList<V>)e.getValue().getTopN(getN()));
      mergedport.emit(tuple);
    }
    kmap.clear();
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void process(HashMap<K, ArrayList<V>> tuple)
  {
    for (Map.Entry<K, ArrayList<V>> e : tuple.entrySet()) {
      TopNSort pqueue = kmap.get(e.getKey());
      ArrayList<V> values = e.getValue();
      if (pqueue == null) {
        pqueue = new TopNSort<V>(5, values.size(), false);
        kmap.put(e.getKey(), pqueue);
        for (int i = (values.size() - 1); i >= 0; i--) {
          pqueue.offer(values.get(i));
        }
      } else {
        for (int i = (values.size() - 1); i >= 0; i--) {
          pqueue.offer(values.get(i));
        }
      }
    }
  }

  /**
   * Sets value of N (depth)
   *
   * @param val
   */
  public void setN(int val)
  {
    n = val;
  }

  /**
   * getter function for N
   *
   * @return n
   */
  @Min(1)
  public int getN()
  {
    return n;
  }
}
