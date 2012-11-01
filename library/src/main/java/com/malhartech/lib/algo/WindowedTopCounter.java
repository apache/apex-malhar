package com.malhartech.lib.algo;

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.*;
import com.malhartech.api.Context.OperatorContext;
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
  @InputPortFieldAnnotation(name = "input")
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
  @OutputPortFieldAnnotation(name = "output")
  public final transient DefaultOutputPort<Map<T, Integer>> output = new DefaultOutputPort<Map<T, Integer>>(this);
  private int windows;
  private int topCount;
  private transient PriorityQueue<WindowedHolder<T>> topCounter;
  private HashMap<T, WindowedHolder> objects;

  public void setSlidingWindowWidth(long slidingWindowWidth, int dagWindowWidth)
  {
    windows = (int)(slidingWindowWidth / dagWindowWidth);
    if (slidingWindowWidth % dagWindowWidth != 0) {
      logger.warn("slidingWindowWidth(" + slidingWindowWidth + ") is not exact multiple of dagWindowWidth(" + dagWindowWidth + ")");
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    topCounter = new PriorityQueue<WindowedHolder<T>>(this.topCount, new TopSpotComparator());
    objects = new HashMap<T, WindowedHolder>(topCount);
  }

  @Override
  public void beginWindow(long windowId)
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
