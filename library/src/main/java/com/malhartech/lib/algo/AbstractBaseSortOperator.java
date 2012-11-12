/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.MutableInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.PriorityQueue;
import javax.validation.constraints.Min;

/**
 * Takes a stream of key value pairs via input port "data". The incoming tuple is merged into already existing sorted list.
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

    MutableInteger count = pmap.get(e);
    if (count == null) {
      count = new MutableInteger(0);
      pmap.put(e, count);
      pqueue.add(e);
    }
    count.value++;
  }

  @Min(1)
  int size = 10;
  protected PriorityQueue<K> pqueue = null;
  protected HashMap<K, MutableInteger> pmap = new HashMap<K, MutableInteger>();

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
   * Cleanup at the start of window
   */
  @Override
  public void beginWindow(long windowId)
  {
    if (pqueue == null) {
      pqueue = new PriorityQueue<K>(size);
    }
    pmap.clear();
    pqueue.clear();
  }

  abstract public boolean doEmitList();
  abstract public boolean doEmitHash();
  abstract public void emitToList(ArrayList<K> list);
  abstract public void emitToHash(HashMap<K,Integer> map);

  /**
   * Emit sorted tuple at end of window
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
      MutableInteger count = pmap.get(o);
      if (sok) {
        for (int i = 0; i < count.value; i++) {
          tuple.add(cloneKey(o));
        }
      }

      if (hok) {
        htuple.put(cloneKey(o), count.value);
      }
    }
    if (sok) {
      emitToList(tuple);
    }
    if (hok) {
      emitToHash(htuple);
    }
  }
}
