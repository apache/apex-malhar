package com.malhartech.demos.twitter;

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.Component;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
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
@ModuleAnnotation(ports = {
  @PortAnnotation(name = Component.INPUT, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = Component.OUTPUT, type = PortAnnotation.PortType.OUTPUT)
})
public class WindowedTopCounter extends AbstractModule
{
  private static final long serialVersionUID = 201208061826L;
  private static final Logger logger = LoggerFactory.getLogger(PartitionedCounter.class);
  private transient int windows;
  private transient int topCount;
  private HashMap<Object, WindowedHolder> objects;
  private PriorityQueue<WindowedHolder> topCounter;

  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException
  {
    topCount = config.getInt("topCount", 10);
    topCounter = new PriorityQueue<WindowedHolder>(this.topCount, new TopSpotComparator());
    objects = new HashMap<Object, WindowedHolder>(topCount);

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
  public void process(Object payload)
  {
    HashMap<Object, Integer> map = (HashMap<Object, Integer>)payload;
    for (Map.Entry<Object, Integer> e: map.entrySet()) {
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

  @Override
  public void endWindow()
  {
    Iterator<Map.Entry<Object, WindowedHolder>> iterator = objects.entrySet().iterator();
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

    HashMap<Object, Integer> map = new HashMap<Object, Integer>();
    Iterator<WindowedHolder> iterator1 = topCounter.iterator();
    while (iterator1.hasNext()) {
      final WindowedHolder wh = iterator1.next();
      map.put(wh.identifier, wh.totalCount);
    }

    emit(Component.OUTPUT, map);
  }

  @Override
  public void teardown()
  {
    topCounter = null;
    objects = null;
  }
}