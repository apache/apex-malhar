/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import javax.validation.constraints.Min;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Takes a stream of key value pairs via input port "data"; The incoming tuple is merged into already existing sorted list.
 * At the end of the window the entire sorted list is emitted on output port "sort"<p>
 * At the end of window all data is flushed. Thus the data set is windowed and no history is kept of previous windows<br>
 * <br>
 * <br>
 *
 * @author amol<br>
 *
 */
//
// TODO: Override PriorityQueue and rewrite addAll to insert with location
//
public abstract class AbstractBaseSortOperator<K> extends BaseKeyOperator<K>
{

  /**
   * Calls processTuple(K) for each item in the list. Override if you need to do this more optimally
   * @param tuple
   */
  public void processTuple(ArrayList<K> tuple)
  {
    for (K e: tuple) {
      processTuple(e);
    }
  }

  /**
   * Inserts each unique key. Updates ref count if key is not unique
   * @param e
   */
  public void processTuple(K e)
  {
    if (e == null) {
      return;
    }

    MutableInt count = pmap.get(e);
    if (count == null) {
      count = new MutableInt(0);
      pmap.put(e, count);
      pqueue.add(e);
    }
    count.increment();
  }

  @Min(1)
  int size = 10;
  protected transient PriorityQueue<K> pqueue = null;
  protected transient HashMap<K, MutableInt> pmap = new HashMap<K, MutableInt>();

  /**
   * getter function for size
   * @return size
   */
  @Min(1)
  public int getSize()
  {
    return size;
  }

  /**
   * Set a size to avoid needing the queue to grow. The queue is purged per window, so the size should be set
   * in sync with number of unique tuples expected per window
   *
   * @param val
   */
  public void setSize(int val)
  {
    size = val;
  }

  /**
   * Initializes queue if not done (once at the start)
   */
  @Override
  public void beginWindow(long windowId)
  {
    if (pqueue == null) {
      initializeQueue();
    }
  }

  public void initializeQueue()
  {
    pqueue = new PriorityQueue<K>(getSize());
  }


  abstract public boolean doEmitList();
  abstract public boolean doEmitHash();
  abstract public void emitToList(ArrayList<K> list);
  abstract public void emitToHash(HashMap<K,Integer> map);

  /**
   * Emit sorted tuple at end of window
   * Clears internal cache
   */
  @Override
  public void endWindow()
  {
    if (pqueue.isEmpty()) {
      return;
    }
    ArrayList tuple = new ArrayList();
    HashMap<K, Integer> htuple = new HashMap<K, Integer>(pqueue.size());
    final boolean sok = doEmitList();
    final boolean hok = doEmitHash();

    if (sok) {
      tuple = new ArrayList();
    }

    if (hok) {
      htuple = new HashMap<K, Integer>(pqueue.size());
    }

    K o;
    while ((o = pqueue.poll()) != null) {
      MutableInt count = pmap.get(o);
      if (sok) {
        for (int i = 0; i < count.intValue(); i++) {
          tuple.add(cloneKey(o));
        }
      }

      if (hok) {
        htuple.put(cloneKey(o), count.intValue());
      }
    }
    if (sok) {
      emitToList(tuple);
    }
    if (hok) {
      emitToHash(htuple);
    }
    pmap.clear();
    pqueue.clear();
  }
}
