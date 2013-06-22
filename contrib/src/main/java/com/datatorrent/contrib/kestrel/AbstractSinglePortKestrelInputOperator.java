/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.kestrel;

import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Kestrel input adapter single port operator, which consume data from Kestrel message bus.<p><br>
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
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractSinglePortKestrelInputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>10 thousand K,V pairs/s</td><td>One tuple per key per window per port</td><td>In-bound rate is the main determinant of performance. Operator can emit about 1 thousand unique (k,v immutable pairs) tuples/sec as Kestrel DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * <br>
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractSinglePortKestrelInputOperator<T> extends AbstractKestrelInputOperator
{
    @OutputPortFieldAnnotation(name = "outputPort")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * Any concrete class derived from AbstractSinglePortKestrelInputOperator has to implement this method
   * so that it knows what type of message it is going to send to Malhar.
   * It converts a byte message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param message
   */
    public abstract T getTuple(byte[] message);

    @Override
    public void emitTuple(byte[] message) {
      outputPort.emit(getTuple(message));
    }
}
