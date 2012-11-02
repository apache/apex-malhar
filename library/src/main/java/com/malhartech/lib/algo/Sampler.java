/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.Random;

/**
 *
 * Takes a stream via input port "data" and emits sample tuple on output port out_data<p>
 * <br>
 * An efficient filter to allow sample analysis of a stream. Very useful is the incoming stream has high throughput<p>
 * <br>
 * <b>Ports</b>
 * <b>data</b>: Input data port expects Object
 * <b>sample</b>: Output data port, emits Object
 * <b>Properties</b>:
 * <b>passrate</b>: Sample rate out of a total of totalrate. Default is 1<br>
 * <b>totalrate</b>: Total rate (divisor). Default is 100<br>
 * <b>Benchmarks></b>: TBD<br>
 * Compile time checks are:<br>
 * None
 * <br>
 * Run time checks are:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator emits  > 65 million tuples/sec.<br>
 * @author amol<br>
 *
 */
public class Sampler<K> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      int fval = random.nextInt(totalrate);
      if (fval >= passrate) {
        return;
      }
      sample.emit(cloneKey(tuple));
    }
  };
  @OutputPortFieldAnnotation(name = "sample")
  public final transient DefaultOutputPort<K> sample = new DefaultOutputPort<K>(this);

  int passrate = 1;
  int totalrate = 100;
  private Random random = new Random();

  public void setPassrate(int val)
  {
    passrate = val;
  }

  public void setTotalrate(int val)
  {
    totalrate = val;
  }
}
