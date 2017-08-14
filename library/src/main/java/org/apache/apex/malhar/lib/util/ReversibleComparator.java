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

import java.util.Comparator;

/**
 *
 * A comparator for ascending and descending lists<p>
 * <br>
 *
 * @since 0.3.2
 */
public class ReversibleComparator<E> implements Comparator<E>
{
  /**
   * Added default constructor for deserializer
   */
  public ReversibleComparator()
  {
  }

  /**
   *
   * @param flag true for ascending, false for descending
   */
  public ReversibleComparator(boolean flag)
  {
    ascending = flag;
  }

  public boolean ascending = true;

  /**
   * Compare function
   * @param e1
   * @param e2
   * @return e1.compareTo(e2) if acscending, else 0 - e1.compareTo(e2)
   */
  @SuppressWarnings("unchecked")
  @Override
  public int compare(E e1, E e2)
  {
    Comparable<? super E> ce1 = (Comparable<? super E>)e1;
    int ret = ce1.compareTo(e2);
    if (!ascending) {
      ret = 0 - ret;
    }
    return ret;
  }
}
