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
package org.apache.apex.malhar.lib.stream;

import java.util.ArrayList;

import org.apache.apex.malhar.lib.util.BaseKeyOperator;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;

/**
 * An implementation of BaseKeyOperator that breaks up an ArrayList tuple into Objects.
 * <p>
 * Takes in an ArrayList and emits each item in the array; mainly used for
 * breaking up an ArrayList tuple into Objects. <br>
 * It is a pass through operator<br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects ArrayList&lt;K&gt;br> <b>item</b>: emits K<br>
 *
 * @displayName Array List To Item
 * @category Stream Manipulators
 * @tags arraylist, clone
 * @since 0.3.3
 */
@Stateless
public class ArrayListToItem<K> extends BaseKeyOperator<K>
{
  /**
   * Input data port that takes an arraylist.
   */
  public final transient DefaultInputPort<ArrayList<K>> data = new DefaultInputPort<ArrayList<K>>()
  {
    /**
     * Emit one item at a time
     */
    @Override
    public void process(ArrayList<K> tuple)
    {
      for (K k : tuple) {
        item.emit(cloneKey(k));
      }
    }
  };

  /**
   * Output port that emits an array item.
   */
  public final transient DefaultOutputPort<K> item = new DefaultOutputPort<K>();
}
