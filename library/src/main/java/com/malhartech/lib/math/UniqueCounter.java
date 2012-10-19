/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.GenericNode;
import com.malhartech.dag.Component;
import com.malhartech.api.Sink;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@ModuleAnnotation(ports = {
  @PortAnnotation( name = Component.INPUT, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation( name = Component.OUTPUT, type = PortAnnotation.PortType.OUTPUT)
})
public class UniqueCounter extends GenericNode implements Sink
{
  /**
   * Bucket counting mechanism.
   * Since we clear the bucket at the beginning of the window, we make this object transient.
   */
  transient HashMap<Object, Integer> map;

  @Override
  public void beginWindow()
  {
    map = new HashMap<Object, Integer>();
  }

  @Override
  public void endWindow()
  {
    emit(map);
  }

  @Override
  public void process(Object payload)
  {
    Integer i = map.get(payload);
    if (i == null) {
      map.put(payload, 1);
    }
    else {
      map.put(payload, i + 1);
    }
  }
}
