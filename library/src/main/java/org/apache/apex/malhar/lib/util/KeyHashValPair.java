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

import java.util.Map;

/**
 * Unlike {@link KeyValPair} this computes hashcode based on just the key and not the value.<br/>
 * Key and Value are to be treated as immutable objects.
 *
 * @since 0.9.0
 */
public class KeyHashValPair<K, V> extends KeyValPair<K, V>
{

  private static final long serialVersionUID = 7005592007894368002L;

  public KeyHashValPair(K k, V v)
  {
    super(k, v);
  }

  private KeyHashValPair()
  {
    super(null, null);
  }

  @Override
  public int hashCode()
  {
    return (getKey() == null ? 0 : getKey().hashCode());
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof Map.Entry)) {
      return false;
    }
    Map.Entry e = (Map.Entry)o;
    return (this.getKey() == null ? e.getKey() == null : this.getKey().equals(e.getKey()));
  }

}
