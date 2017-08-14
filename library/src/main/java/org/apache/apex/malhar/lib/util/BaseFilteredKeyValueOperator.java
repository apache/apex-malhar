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
package org.apache.apex.malhar.lib.util;

import java.util.HashMap;

/**
 * This operator filters incoming tuples based on whether or not the tuple is contained in its filter set.
 * <p>
 * By default no filtering would be done as inverse is set to true and filterBy would be empty unless set<br>
 * <br>
 * <b>Ports</b>: None<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>inverse</b>: if set to true the key in the filter will block tuple. Default is true<br>
 * <b>filterBy</b>: List of keys to filter on. Default is an empty filter<br>
 * <br>
 * <b>Specific compile time checks</b>: None<br>
 * <b>Specific run time checks</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Not done as there are no ports on this operator<br>
 * </p>
 * @displayName Base Filtered Key Value
 * @category Algorithmic
 * @tags filter, key value
 * @since 0.3.2
 */
public class BaseFilteredKeyValueOperator<K, V> extends BaseKeyValueOperator<K, V>
{
  private HashMap<K, Object> filterBy = new HashMap<K, Object>(4);
  private boolean inverse = true;

  /**
   * getter function for inverse
   * @return the value of inverse
   */
  public boolean getInverse()
  {
    return inverse;
  }

  /**
   * Setter function for inverse. The filter is a negative filter is inverse is set to true
   * @param i value of inverse
   */
  public void setInverse(boolean i)
  {
    inverse = i;
  }

  /**
   * setter function for filterBy
   * @param list list of keys for subtoken filters
   */
  public void setFilterBy(K[] list)
  {
    if (list != null) {
      for (K s: list) {
        filterBy.put(s, null);
      }
    }
  }

  public boolean doprocessKey(K key)
  {
    boolean fcontains = filterBy.containsKey(key);
    return (!inverse && fcontains) || (inverse && !fcontains);
  }
}
