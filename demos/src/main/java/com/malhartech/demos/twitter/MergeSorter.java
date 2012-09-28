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
import java.util.PriorityQueue;

// how do we support n number of input streams - need syntax for it.
/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@ModuleAnnotation(ports = {
  @PortAnnotation(name = Component.INPUT, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = Component.OUTPUT, type = PortAnnotation.PortType.OUTPUT)
})
public class MergeSorter extends AbstractModule
{
  private static final long serialVersionUID = 201208061824L;
  private PriorityQueue<WindowedHolder> topCounter;
  private int topCount;

  @Override
  public void setup(ModuleConfiguration config)
  {
    topCount = config.getInt("topCount", 10);
    topCounter = new PriorityQueue<WindowedHolder>(this.topCount, new TopSpotComparator());
  }

  @Override
  public void beginWindow()
  {
    topCounter.clear();
  }

  @Override
  public void process(Object payload)
  {
    WindowedHolder wuh = (WindowedHolder)payload;

    if (topCounter.size() == topCount) {
      if (wuh.totalCount > topCounter.peek().totalCount) {
        topCounter.poll();
        topCounter.add(wuh);
      }
    }
    else {
      topCounter.add(wuh);
    }
  }

  @Override
  public void endWindow()
  {
    WindowedHolder wuh;
    while ((wuh = topCounter.poll()) != null) {
      emit(Component.OUTPUT, wuh);
    }
  }
}
