/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.demos.performance;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.annotation.PortAnnotation.PortType;
import com.malhartech.dag.InputModule;
import com.malhartech.dag.Component;
import com.malhartech.api.Sink;
import com.malhartech.dag.Tuple;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@ModuleAnnotation(ports = {
  @PortAnnotation(name = Component.OUTPUT, type = PortType.OUTPUT)
})
public class RandomWordInputModule extends InputModule implements Sink<Tuple>
{
  long lastWindowId = 0;
  int count = 1;
//  int totalIterations = 0;

  @Override
  public final void process(Tuple tuple)
  {
    if (tuple.getWindowId() == lastWindowId) {
      emit(new byte[64]);
      count++;
    }
    else {
      for (int i = count--; i-- > 0;) {
        emit(new byte[64]);
      }
      lastWindowId = tuple.getWindowId();
//      if (++totalIterations > 20) {
//        deactivate();
//      }
    }
  }
}
