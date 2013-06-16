/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.memcache;

import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultOutputPort;
import java.util.Map.Entry;

/**
 * Memcache input adapter single port operator, which get Data from Memcached using spymemcached library.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have one output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method getTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: TBD
 * <br>
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractSinglePortMemcacheInputOperator<T> extends AbstractMemcacheInputOperator
{
  @OutputPortFieldAnnotation(name = "outputPort")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>(this);

  /**
   * Any concrete class derived from AbstractSinglePortMemcacheInputOperator has to implement this method
   * so that it knows what type of data it will receive from Memcached
   * It converts a byte message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param message
   */
  public abstract T getTuple(Object o);

  @Override
  public void emitTuple(Object o) {
    T t = getTuple(o);
    if( t != null ) {
      outputPort.emit(t);
    }
  }
}
