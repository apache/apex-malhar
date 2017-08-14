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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import javax.validation.constraints.Min;

/**
 *
 * Gives top N objects in ascending or descending order<p>
 * This class is more efficient that just using PriorityQueue and then picking up the top N. The class works by not even inserting objects
 * that would not make it to top N. There is no API to look at top of the list at any given time as per design. The aim is for users to only take the topN
 * once all the inserts are done<br>
 *
 * @since 0.3.2
 */
public class TopNSort<E>
{
  @Min(1)
  int qbound = Integer.MAX_VALUE;
  boolean ascending = true;
  PriorityQueue<E> q = null;

  /**
   * getter function for qbound
   * @return qbound
   */
  @Min(1)
  public int getQbound()
  {
    return qbound;
  }

  /**
   * setter function for qbound
   * @param i
   */
  public void setQbound(int i)
  {
    qbound = i;
  }

  /**
   * Added default constructor for deserializer
   */
  public TopNSort()
  {
  }

  /**
   * Constructs and sets values accordingly
   * @param initialCapacity
   * @param bound
   * @param flag
   */
  public TopNSort(int initialCapacity, int bound, boolean flag)
  {
    ascending = flag;
    // Ascending use of pqueue needs a descending comparator
    q = new PriorityQueue<E>(initialCapacity, new ReversibleComparator<E>(flag));
    qbound = bound;
  }

  /**
   * adds an object
   * @param e
   * @return true is add succeeds
   */
  public boolean add(E e)
  {
    return offer(e);
  }

  /**
   * Size of the queue
   * @return size of the priority queue
   */
  public int size()
  {
    return q.size();
  }

  /**
   * Clears the queue
   */
  public void clear()
  {
    q.clear();
  }

  /**
   *
   * @return true if queue is empty
   */
  public boolean isEmpty()
  {
    return q.isEmpty();
  }

  /**
   * Returns topN objects
   * @param n
   * @return ArrayList of top N object
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public List<E> getTopN(int n)
  {
    List<E> list = new ArrayList<E>();
    E v;
    int j = 0;
    while (((v = q.poll()) != null) && (j < n)) {
      list.add(v);
      j++;
    }
    if (list.isEmpty()) {
      return list;
    }

    Collections.reverse(list);
    return list;
    //return ret;
  }

  /**
   * Adds object
   * @param e object to be added
   * @return true is offer() succeeds
   */
  @SuppressWarnings("unchecked")
  public boolean offer(E e)
  {
    if (q.size() < qbound) {
      return q.offer(e);
    }

    boolean ret = false;
    boolean insert;
    Comparable<? super E> head = (Comparable<? super E>)q.peek();

    if (ascending) { // means head is the lowest value due to inversion
      insert = head.compareTo(e) <= 0; // e >= head
    } else { // means head is the highest value due to inversion
      insert = head.compareTo(e) >= 0; // head is <= e
    }
    if (insert && q.offer(e)) {
      ret = true;
      q.poll();
    }
    return ret;
  }
}
