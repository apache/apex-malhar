/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.api.annotation.InputPortFieldAnnotation;
import com.malhartech.api.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.lib.util.BaseKeyOperator;
import java.util.Random;
import javax.validation.constraints.Min;

/**
 *
 * Emits sample percentage tuples<p>
 * <br>
 * An efficient filter to allow sample analysis of a stream. Very useful is the incoming stream has high throughput<p>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects K<br>
 * <b>sample</b>: emits K<br>
 * <br>
 * <b>Properties</b>:<br>
 * <b>passrate</b>: Sample rate out of a total of totalrate. Default is 1<br>
 * <b>totalrate</b>: Total rate (divisor). Default is 100<br>
 * <br>
 * <b>Specific compile time checks are</b>: None<br>
 * passrate is positive integer<br>
 * totalrate is positive integer<br>
 * passrate and totalrate are not compared (i.e. passrate &lt; totalrate) check is not done to allow users to make this operator a passthrough (all) during testing<br>
 * <br>
 * <b>Specific run time checks are</b>: None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for Sampler&lt;K&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>&gt; 65 Million tuples/s</b></td><td>Randomly selected passrate/totalrate percent tuples per window</td>
 * <td>In-bound throughput and passrate percentage are the main determinant of performance.
 * Tuples are assumed to be immutable. If you use mutable tuples and have lots of keys, the benchmarks may be lower</td></tr>
 * </table><br>
 * <p>
 * <b>Function Table (K=HashMap&lt;String,Integer&gt;); passrate=1, totalrate=4</b>: The selection of which tuple was emitted is at a random probability of 25%
 * <table border="1" cellspacing=1 cellpadding=1 summary="Function table for Sampler&lt;K&gt; operator template">
 * <tr><th rowspan=2>Tuple Type (api)</th><th>In-bound (process)</th><th>Out-bound (emit)</th></tr>
 * <tr><th><i>data</i>(K)</th><th><i>sample</i>(K)</th></tr>
 * <tr><td>Begin Window (beginWindow())</td><td>N/A</td><td>N/A</td></tr>
 * <tr><td>Data (process())</td><td>{a=2,b=20,c=1000}</td><td>{a=2,b=20,c=1000}</td></tr>
 * <tr><td>Data (process())</td><td>{a=-1}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=10,b=5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=5,b=-5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{a=2,d=14,h=20,c=2,b=-5}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=55,b=12}</td><td></td></tr>
 * <tr><td>Data (process())</td><td>{d=22,b=5}</td><td>{d=22,b=5}</td></tr>
 * <tr><td>Data (process())</td><td>{d=14}</td><td></td></tr>
 * <tr><td>End Window (endWindow())</td><td>N/A</td><td>N/A</td></tr>
 * </table>
 * <br>
 * @author Amol Kekre (amol@malhar-inc.com)<br>
 * <br>
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
  @Min(1)
  int totalrate = 100;
  private transient Random random = new Random();

  /**
   * getter function for pass rate
   * @return passrate
   */
  @Min(1)
  public int getPassrate()
  {
    return passrate;
  }

  /**
   * getter function for total rate
   * @return totalrate
   */
  @Min(1)
  public int getTotalrate()
  {
    return totalrate;
  }

  /**
   * Sets pass rate
   *
   * @param val passrate is set to val
   */
  public void setPassrate(int val)
  {
    passrate = val;
  }

  /**
   * Sets total rate
   *
   * @param val total rate is set to val
   */
  public void setTotalrate(int val)
  {
    totalrate = val;
  }
}
