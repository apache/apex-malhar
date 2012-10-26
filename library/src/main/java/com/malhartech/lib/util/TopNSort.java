/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.google.common.collect.MinMaxPriorityQueue;
import java.util.ArrayList;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Gives top N objects in ascending or descending order<p>
 * This class is more efficient that just using PriorityQueue and then picking up the top N. The class works by not even inserting objects
 * that would not make it to top N. There is no API to look at top of the list at any given time as per design. The aim is for users to only take the topN
 * once all the inserts are done<br>
 *
 *
 * @author amol<br>
 *
 */
public class TopNSort<E>
{
  private static Logger LOG = LoggerFactory.getLogger(TopNSort.class);
  int qbound = Integer.MAX_VALUE;
  boolean ascending = true;

  //MinMaxPriorityQueue<E> mq = null;
  PriorityQueue<E> q = null;

  public TopNSort(int initialCapacity, int bound, boolean flag)
  {
    ascending = flag;
    // Ascending use of pqueue needs a descending comparator
    q = new PriorityQueue<E>(initialCapacity, new ReversibleComparator<E>(!flag));
  }

  public boolean add(E e)
  {
    return offer(e);
  }

  public int size() {
    return q.size();
  }

  public void clear() {
    q.clear();
  }

  public boolean isEmpty() {
    return q.isEmpty();
  }

  public ArrayList getTopN(int n)
  {
    ArrayList list = new ArrayList();
    E v;
    int j = 0;
    while ((v = q.poll()) != null) {
      list.add(v);
      j++;
      if (j > n) {
        break;
      }
    }
    if (list.isEmpty()) {
      return list;
    }

    ArrayList ret = new ArrayList(list.size());
    int size = list.size();
    if (size > n) {
      size = n;
    }
    for (int i = 0; i < size; i++) {
      ret.add(list.get(i));
    }
    return ret;
  }

  public boolean offer(E e)
  {
    if (q.size() <= qbound) {
      return q.offer(e);
    }

    boolean ret = true;
    boolean insert;
    Comparable<? super E> head = (Comparable<? super E>) q.peek();

    if (ascending) { // means head is the lowest value due to inversion
      insert = head.compareTo(e) <= 0; // e >= head
    }
    else { // means head is the highest value due to inversion
      insert = head.compareTo(e) >= 0; // head is <= e
    }

    if (insert && q.offer(e)) {
      ret = true;
      q.poll();
    }
    return ret;
  }
}
