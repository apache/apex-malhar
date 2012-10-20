package com.malhartech.lib.algo;

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
import com.malhartech.api.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class WindowedTopCounter<T> extends BaseOperator
{
  private static final long serialVersionUID = 201208061826L;
  private static final Logger logger = LoggerFactory.getLogger(WindowedTopCounter.class);
  public final transient DefaultInputPort<Map<T, Integer>> input = new DefaultInputPort<Map<T, Integer>>(this)
  {
    @Override
    public void process(Map<T, Integer> map)
    {
      for (Map.Entry<T, Integer> e: map.entrySet()) {
        WindowedHolder holder = objects.get(e.getKey());
        if (holder == null) {
          holder = new WindowedHolder(e.getKey(), windows);
          holder.totalCount = e.getValue();
          objects.put(e.getKey(), holder);
        }
        else {
          holder.adjustCount(e.getValue());
        }
      }
    }
  };
  public final transient DefaultOutputPort<Map<T, Integer>> output = new DefaultOutputPort<Map<T, Integer>>(this);
  private transient int windows;
  private transient int topCount;
  private transient PriorityQueue<WindowedHolder<T>> topCounter;
  private HashMap<T, WindowedHolder> objects;

  @Override
  public void setup(OperatorConfiguration config) throws FailedOperationException
  {
    topCount = config.getInt("topCount", topCount);
    topCounter = new PriorityQueue<WindowedHolder<T>>(this.topCount, new TopSpotComparator());
    objects = new HashMap<T, WindowedHolder>(topCount);

    long windowWidth = config.getInt("windowWidth", 500);
    long samplePeriod = config.getInt("samplePeriod", 300000);
    windows = (int)(samplePeriod / windowWidth);

    if (samplePeriod % windowWidth != 0) {
      logger.warn("samplePeriod(" + samplePeriod + ") is not exact multiple of windowWidth(" + windowWidth + ")");
    }
  }

  @Override
  public void beginWindow()
  {
    topCounter.clear();
  }

  @Override
  public void endWindow()
  {
    Iterator<Map.Entry<T, WindowedHolder>> iterator = objects.entrySet().iterator();
    int i = topCount;

    /*
     * Try to fill the priority queue with the first topCount URLs.
     */
    WindowedHolder holder;
    while (iterator.hasNext()) {
      holder = iterator.next().getValue();
      holder.slide();

      if (holder.totalCount == 0) {
        iterator.remove();
      }
      else {
        topCounter.add(holder);
        if (--i == 0) {
          break;
        }
      }
    }

    /*
     * Make room for the new element in the priority queue by deleting the
     * smallest one, if we KNOW that the new element is useful to us.
     */
    if (i == 0) {
      int smallest = topCounter.peek().totalCount;
      while (iterator.hasNext()) {
        holder = iterator.next().getValue();
        holder.slide();

        if (holder.totalCount > smallest) {
          topCounter.poll();
          topCounter.add(holder);
          smallest = topCounter.peek().totalCount;
        }
        else if (holder.totalCount == 0) {
          iterator.remove();
        }
      }
    }

    /*
     * Emit our top URLs without caring for order.
     */
    HashMap<T, Integer> map = new HashMap<T, Integer>();
    Iterator<WindowedHolder<T>> iterator1 = topCounter.iterator();
    while (iterator1.hasNext()) {
      final WindowedHolder<T> wh = iterator1.next();
      map.put(wh.identifier, wh.totalCount);
    }

    output.emit(map);
  }

  @Override
  public void teardown()
  {
    topCounter = null;
    objects = null;
  }

  public void setTopCount(int i)
  {
    topCount = i;
  }
}
