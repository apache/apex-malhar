/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.util;

import com.malhartech.lib.algo.*;
import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * extends {@link java.util.PriorityQueue} to enforce bounds (upper limit). This helps performance as it allows top N objects
 * to be kept and thus keeps N low.<br>
 *
 *
 * @author amol<br>
 *
 */
public class BoundedPriorityQueue<E> extends PriorityQueue<E>
{
  private static Logger LOG = LoggerFactory.getLogger(TopN.class);

  int qbound = Integer.MAX_VALUE;


  public BoundedPriorityQueue(int initialCapacity, int bound)
  {
    super(initialCapacity, null);
    qbound = bound;
  }

  @Override
  public boolean add(E e)
  {
    return offer(e);
  }

  @Override
  public boolean offer(E e)
  {
    boolean ret = super.offer(e);
    if (size() >= qbound) {
      poll();
    }
    return ret;
  }
}
