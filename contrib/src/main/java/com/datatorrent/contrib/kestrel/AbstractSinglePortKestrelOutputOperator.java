/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.kestrel;

import com.datatorrent.api.DefaultInputPort;

/**
 * Kestrel output adapter operator, which send data to Kestrel message bus.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have one input port<br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractSinglePortKestrelOutputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td>One tuple per key per window per port</td><td><b>10 thousand K,V pairs/s</td><td>Out-bound rate is the main determinant of performance. Operator can process about 1 thousand unique (k,v immutable pairs) tuples/sec as Kestrel DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * <br>
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractSinglePortKestrelOutputOperator<T> extends AbstractKestrelOutputOperator
{

  /**
   * Users need to provide conversion of Tuple to message
   * @param tuple
   */
  public abstract void processTuple(T tuple);

  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple); // This is an abstract call
    }
  };
}
