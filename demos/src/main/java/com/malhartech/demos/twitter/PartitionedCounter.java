/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.Component;
import com.malhartech.dag.ModuleConfiguration;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
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
public class PartitionedCounter extends AbstractModule
{
  private static final long serialVersionUID = 201208061826L;
  private static final Logger logger = LoggerFactory.getLogger(PartitionedCounter.class);
  private HashMap<ByteBuffer, WindowedHolder> urls;
  private PriorityQueue<WindowedHolder> topCounter;
  private int topCount;
  private int windows;
  private boolean merge;

  @Override
  public void setup(ModuleConfiguration config)
  {
    urls = new HashMap<ByteBuffer, WindowedHolder>();

    this.topCount = config.getInt("topCount", 10);
    topCounter = new PriorityQueue<WindowedHolder>(this.topCount, new TopSpotComparator());

    long windowWidth = config.getInt("windowWidth", 500);
    long samplePeriod = config.getInt("samplePeriod", 300000);
    this.windows = (int)(samplePeriod / windowWidth);

    if (samplePeriod % windowWidth != 0) {
      logger.warn("samplePeriod(" + samplePeriod + ") is not exact multiple of windowWidth(" + windowWidth + ")");
      samplePeriod = this.windows * windowWidth;
    }

    merge = config.getBoolean("merge", false);
    logger.info("samplePeriod = " + samplePeriod + "; windowWidth = " + windowWidth + " merge = " + merge);
  }

  @Override
  public void process(Object payload)
  {
    ByteBuffer bb = ByteBuffer.wrap((byte[])payload);
    WindowedHolder holder = urls.get(bb);
    if (holder == null) {
      holder = new WindowedHolder(bb, windows);
      holder.totalCount = 1;
      urls.put(bb, holder);
    }
    else {
      holder.adjustCount(1);
    }
  }

  @Override
  public void endWindow()
  {
    Iterator<Entry<ByteBuffer, WindowedHolder>> iterator = urls.entrySet().
            iterator();
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
    Iterator<WindowedHolder> iterator1 = topCounter.iterator();
    while (iterator1.hasNext()) {
      emit(Component.OUTPUT, iterator1.next().clone());
    }

    topCounter.clear();
  }

  @Override
  public void teardown()
  {
    urls = null;
    topCounter = null;
  }
}
