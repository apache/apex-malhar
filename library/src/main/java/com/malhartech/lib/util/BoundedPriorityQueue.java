/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.Comparator;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Provides BoundedPriorityQueue functionality. Users can insert objects of type E. This is a wrapper around PriorityQueue<br>
 * This class is more efficient that just using PriorityQueue and then picking up the top N. The class works by not even inserting objects
 * that would not make it to top N.
 *
 * @author amol<br>
 *
 */
public class BoundedPriorityQueue<E>
{
  private static Logger LOG = LoggerFactory.getLogger(BoundedPriorityQueue.class);

  int qbound = Integer.MAX_VALUE;
  boolean ascending = true;

  PriorityQueue<E> q = null;

  public BoundedPriorityQueue(int initialCapacity, int bound)
  {
    q = new PriorityQueue<E>(initialCapacity, null);
    qbound = bound;
  }

  public void setBound(int i) {
    qbound = i;
  }

  public void setAscending()
  {
    ascending = true;
  }

  public void setDescending()
  {
    ascending = false;
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

  public E peek(){
    return q.peek();
  }

   public E poll(){
    return q.poll();
  }

  public boolean offer(E e)
  {
    if (q.size() <= qbound) {
      return q.offer(e);
    }

    boolean ret = true;
    Comparator<? super E> cmp = q.comparator();
    E head = q.peek();
    boolean insert = false;

    if (cmp != null) {
      if (ascending) {
        insert = cmp.compare(head, (E)e) >= 0; // head is >= e
      }
      else {
        insert = cmp.compare(head, (E)e) <= 0; // head is <= e
      }
    }
    else {
      Comparable<? super E> key = (Comparable<? super E>)head;
      if (ascending) {
        insert = key.compareTo((E)e) >= 0; // head is >= e
      }
      else {
        insert = key.compareTo((E)e) <= 0; // head is <= e
      }
    }

    if (q.offer(e)) {
      ret = true;
      q.poll();
    }
    return ret;
  }
}
