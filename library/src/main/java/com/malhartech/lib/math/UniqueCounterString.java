/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.NodeAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractNode;
import com.malhartech.dag.Component;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@NodeAnnotation(ports = {
  @PortAnnotation( name = Component.INPUT, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation( name = Component.OUTPUT, type = PortAnnotation.PortType.OUTPUT)
})
public class UniqueCounterString extends AbstractNode
{
  HashMap<String, Integer> map = new HashMap<String, Integer>();

  @Override
  public void beginWindow()
  {
    map.clear();
  }

  @Override
  public void endWindow()
  {
    for (Entry<String, Integer> e: map.entrySet()) {
      emit(e);
    }
  }

  @Override
  public void process(Object payload)
  {
    Integer i = map.get((String)payload);
    if (i == null) {
      map.put((String)payload, 1);
    }
    else {
      map.put((String)payload, i + 1);
    }
  }
}
