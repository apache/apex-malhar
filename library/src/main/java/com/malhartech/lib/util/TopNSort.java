/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.ArrayList;
import java.util.Comparator;
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

  class myComparator<E> implements Comparator<E>
  {
    public myComparator(boolean flag)
    {
      ascending = flag;
    }
    public boolean ascending = true;

    @Override
    public int compare(E e1, E e2)
    {
      Comparable<? super E> ce1 = (Comparable<? super E>) e1;
      int ret = ce1.compareTo(e2);
      if (ascending) {
        ret = 0 - ret;
      }
      return ret;
    }
  }

  PriorityQueue<E> q = null;

  public TopNSort(int initialCapacity, int bound, boolean flag)
  {
    ascending = flag;
    q = new PriorityQueue<E>(initialCapacity, new myComparator<E>(flag));
    qbound = bound;
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

  public ArrayList getTopN()
  {
    ArrayList list = new ArrayList();
    E v;
    while ((v = q.poll()) != null) {
      list.add(v);
    }
    ArrayList ret = new ArrayList(list.size());
    int size = list.size();
    for (int i = size; i > 0; i--) {
      ret.add(size-i, list.get(i));
    }
    return ret;
  }


  //
  // If ascending, put the order in reverse
  //
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
