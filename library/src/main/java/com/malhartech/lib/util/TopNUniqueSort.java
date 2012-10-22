/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static Logger LOG = LoggerFactory.getLogger(TopNUniqueSort.class);
  int qbound = Integer.MAX_VALUE;
  boolean ascending = true;

  HashMap<E, MutableInteger> hmap = null;

  PriorityQueue<E> q = null;

  public TopNUniqueSort(int initialCapacity, int bound, boolean flag)
  {
    ascending = flag;
    // Ascending use of pqueue needs a descending comparator
    q = new PriorityQueue<E>(initialCapacity, new ReversibleComparator<E>(!flag));
    qbound = bound;
    hmap = new HashMap<E, MutableInteger>();
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

  /*
   * Returns ArrayList<HashMap<E, Integer>>
   */
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
      E o = (E) list.get(i);
      HashMap<E, Integer> val = new HashMap<E, Integer>(1);
      MutableInteger ival = hmap.get(o);
      val.put(o, ival.value);
      ret.add(size-i, val);
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
    //
    // object e makes it, someone else gets dropped
    //
    ival = new MutableInteger(0);
    ival.value = 1;
    hmap.put(e, ival);
    if (insert && q.offer(e)) {
      ret = true;
      // the dropped object will never make it to back in anymore
      E drop = q.poll();
      hmap.remove(drop);
    }
    return ret;
  }
}
