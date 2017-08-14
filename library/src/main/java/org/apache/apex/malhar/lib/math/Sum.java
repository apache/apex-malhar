/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.math;

import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.util.BaseNumberValueOperator;
import org.apache.apex.malhar.lib.util.UnifierSumNumber;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * This operator implements Unifier interface and emits the sum of values at the end of window.
 * <p>
 * This is an end of window operator. Application can turn this into accumulated
 * sum operator by setting cumulative flag to true. <br>
 * <b>StateFull : Yes</b>, sum is computed over application window >= 1. <br>
 * <b>Partitions : Yes</b>, sum is unified at output port. <br>
 * <br>
 * <b>Ports</b>:<br>
 * <b>data</b>: expects V extends Number<br>
 * <b>sum</b>: emits V extends Number<br>
 * <b>sumDouble</b>: emits Double<br>
 * <b>sumFloat</b>: emits Float<br>
 * <b>sumInteger</b>: emits Integer<br>
 * <b>sumLong</b>: emits Long<br>
 * <b>sumShort</b>: emits Short<br>
 * <br>
 * <b>Properties: </b> <br>
 * <b>cumulative </b> Sum has to be cumulative. <br>
 * <br>
 * @displayName Sum
 * @category Math
 * @tags numeric, sum
 * @param <V>
 *          Generic number type parameter. <br>
 * @since 0.3.3
 */
public class Sum<V extends Number> extends BaseNumberValueOperator<V> implements
    Unifier<V>
{
  /**
   * Sum value.
   */
  protected double sums = 0;

  /**
   * Input tuple processed flag.
   */
  protected boolean tupleAvailable = false;

  /**
   * Accumulate sum flag.
   */
  protected boolean cumulative = false;

  /**
   * Input port to receive data.&nbsp; It computes sum and count for each tuple.
   */
  public final transient DefaultInputPort<V> data = new DefaultInputPort<V>()
  {
    /**
     * Computes sum and count with each tuple
     */
    @Override
    public void process(V tuple)
    {
      Sum.this.process(tuple);
      tupleAvailable = true;
    }
  };

  /**
   * Unifier process override.
   */
  @Override
  public void process(V tuple)
  {
    sums += tuple.doubleValue();
    tupleAvailable = true; // also need to set here for Unifier
  }

  /**
   * Output sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<V> sum = new DefaultOutputPort<V>()
  {
    @Override
    public Unifier<V> getUnifier()
    {
      UnifierSumNumber<V> ret = new UnifierSumNumber<V>();
      ret.setVType(getType());
      return ret;
    }
  };

  /**
   * Output double sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Double> sumDouble = new DefaultOutputPort<Double>()
  {
    @Override
    public Unifier<Double> getUnifier()
    {
      UnifierSumNumber<Double> ret = new UnifierSumNumber<Double>();
      ret.setType(Double.class);
      return ret;
    }
  };

  /**
   * Output integer sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Integer> sumInteger = new DefaultOutputPort<Integer>()
  {
    @Override
    public Unifier<Integer> getUnifier()
    {
      UnifierSumNumber<Integer> ret = new UnifierSumNumber<Integer>();
      ret.setType(Integer.class);
      return ret;
    }
  };

  /**
   * Output Long sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Long> sumLong = new DefaultOutputPort<Long>()
  {
    @Override
    public Unifier<Long> getUnifier()
    {
      UnifierSumNumber<Long> ret = new UnifierSumNumber<Long>();
      ret.setType(Long.class);
      return ret;
    }
  };

  /**
   * Output short sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Short> sumShort = new DefaultOutputPort<Short>()
  {
    @Override
    public Unifier<Short> getUnifier()
    {
      UnifierSumNumber<Short> ret = new UnifierSumNumber<Short>();
      ret.setType(Short.class);
      return ret;
    }
  };

  /**
   * Output float sum port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Float> sumFloat = new DefaultOutputPort<Float>()
  {
    @Override
    public Unifier<Float> getUnifier()
    {
      UnifierSumNumber<Float> ret = new UnifierSumNumber<Float>();
      ret.setType(Float.class);
      return ret;
    }
  };

  /**
   * Redis server output port.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<Map<Integer, Integer>> redisport = new DefaultOutputPort<Map<Integer, Integer>>();

  /**
   * Check if sum has to be cumulative.
   *
   * @return cumulative flag
   */
  public boolean isCumulative()
  {
    return cumulative;
  }

  /**
   * Set cumulative flag.
   *
   * @param cumulative
   *          flag
   */
  public void setCumulative(boolean cumulative)
  {
    this.cumulative = cumulative;
  }

  /**
   * Emits sum and count if ports are connected
   */
  @Override
  public void endWindow()
  {
    if (doEmit()) {
      sum.emit(getValue(sums));
      sumDouble.emit(sums);
      sumInteger.emit((int)sums);
      sumLong.emit((long)sums);
      sumShort.emit((short)sums);
      sumFloat.emit((float)sums);
      tupleAvailable = false;
      Map<Integer, Integer> redis = new HashMap<Integer, Integer>();
      redis.put(1, (int)sums);
      redisport.emit(redis);
    }
    clearCache();
  }

  /**
   * Clears the cache making this operator stateless on window boundary
   */
  private void clearCache()
  {
    if (!cumulative) {
      sums = 0;
    }
  }

  /**
   * Decides whether emit has to be done in this window on port "sum"
   *
   * @return true is sum port is connected
   */
  private boolean doEmit()
  {
    return tupleAvailable;
  }
}
