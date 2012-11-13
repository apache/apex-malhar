/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.lib.util.BaseKeyOperator;
import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.Random;
import javax.validation.constraints.Min;

/**
 *
 * Emits sample percentage tuples<p>
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
 * Operator emits > 65 million tuples/sec.<br>
 *
 * @author amol<br>
 *
 */
public class Sampler<K> extends BaseKeyOperator<K>
{
  @InputPortFieldAnnotation(name = "data")
  public final transient DefaultInputPort<K> data = new DefaultInputPort<K>(this)
  {
    /**
     * Emits the tuple as per probability of passrate out of totalrate
     */
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
  @Min(1)
  int passrate = 1;
  int totalrate = 100;
  private Random random = new Random();

  /**
   * Returns pass rate
   */
  public int getPassrate()
  {
    return passrate;
  }

  /**
   * returns total rate
   *
   * @return
   */
  public int getTotalrate()
  {
    return totalrate;
  }

  /**
   * Sets pass rate
   *
   * @param val
   */
  public void setPassrate(int val)
  {
    passrate = val;
  }

  /**
   * Sets total rate
   *
   * @param val
   */
  public void setTotalrate(int val)
  {
    totalrate = val;
  }
}
