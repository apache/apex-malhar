/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

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
 *
 * @author amol<br>
 *
 */
public class Sampler<K> extends BaseOperator
{
  int passrate_default_value = 1;
  int totalrate_default_value = 100;
  int passrate = passrate_default_value;
  int totalrate = totalrate_default_value;
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      int fval = random.nextInt(totalrate);
      if (fval >= passrate) {
        return;
      }
      sample.emit(tuple);
    }
  };
  private Random random = new Random();
  public final transient DefaultOutputPort<K> sample = new DefaultOutputPort<K>(this);

  public void setPassrate(int val)
  {
    passrate = val;
  }

  public void setTotalrate(int val)
  {
    totalrate = val;
  }
}
