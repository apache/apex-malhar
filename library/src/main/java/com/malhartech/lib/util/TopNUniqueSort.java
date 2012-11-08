/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;

/**
 *
 * Gives top N objects in ascending or descending order and counts only unique objects<p>
 * This class is more efficient that just using PriorityQueue and then picking up the top N. The class works by not even inserting objects
 * that would not make it to top N. There is no API to look at top of the list at any given time as per design. The aim is for users to only take the topN
 * once all the inserts are done<br>
 *
 *
 * @author amol<br>
 *
 */
public class TopNUniqueSort<E>
{
  int qbound = Integer.MAX_VALUE;
  boolean ascending = true;
  HashMap<E, MutableInteger> hmap = null;
  PriorityQueue<E> q = null;

  /**
   * Added default constructor for deserializer
   */
  public TopNUniqueSort()
  {
  }

  public TopNUniqueSort(int initialCapacity, int bound, boolean flag)
  {
    ascending = flag;
    // Ascending use of pqueue needs a descending comparator
    q = new PriorityQueue<E>(initialCapacity, new ReversibleComparator<E>(flag));
    qbound = bound;
    hmap = new HashMap<E, MutableInteger>();
  }

  public boolean add(E e)
  {
    return offer(e);
  }

  public int size()
  {
    return q.size();
  }

  public void clear()
  {
    q.clear();
  }

  public boolean isEmpty()
  {
    return q.isEmpty();
  }

  /*
   * Returns ArrayList<HashMap<E, Integer>>
   */
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
    ArrayList ret = new ArrayList(list.size());
    int size = list.size();
    int depth = size;
    if (depth > n) {
      depth = n;
    }
    for (int i = 0; i < depth; i++) {
      E o = (E) list.get(size - i - 1);
      HashMap<E, Integer> val = new HashMap<E, Integer>(1);
      MutableInteger ival = hmap.get(o);
      val.put(o, ival.value);
      ret.add(val);
    }
    return ret;
  }

  //
  // If ascending, put the order in reverse
  //
  public boolean offer(E e)
  {
    MutableInteger ival = hmap.get(e);
    if (ival != null) { // already exists, so no insertion
      ival.value++;
      return true;
    }
    if (q.size() <= qbound) {
      if (ival == null) {
        hmap.put(e, new MutableInteger(1));
      }
      return q.offer(e);
    }

    boolean ret = true;
    boolean insert;
    Comparable<? super E> head = (Comparable<? super E>) q.peek();

    if (ascending) { // means head is the lowest value due to inversion
      insert = head.compareTo(e) < 0; // e > head
    }
    else { // means head is the highest value due to inversion
      insert = head.compareTo(e) > 0; // head is < e
    }

    // object e makes it, someone else gets dropped
    if (insert && q.offer(e)) {
      hmap.put(e, new MutableInteger(1));
      ret = true;

      // the dropped object will never make it to back in anymore
      E drop = q.poll();
      hmap.remove(drop);
    }
    return ret;
  }
}
