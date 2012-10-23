/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.OperatorConfiguration;
import com.malhartech.lib.util.MutableDouble;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Takes in two streams via input ports "numerator" and "denominator". At the
 * end of window computes the quotient for all the tuples and emits the result on port "quotient".<p>
 * <br>
 * <b>Ports</b>:
 * <b>numerator</b> expects V extends Number<br>
 * <b>denominator</b> expects V extends Number<br>
 * <b>quotient</b> emits Double<br>
 * <br>
 * <br>
 * <b>Compile time checks</b>
 * None<br>
 * <br>
 * <b>Runtime checks</b>
 * None<br>
 * <br>
 * <b>Benchmarks</b><br>
 * <br>
 * Benchmarks:<br>
 * With HashMap schema the node does about 3 Million/tuples per second<br>
 * <br>
 *
 * @author amol<br>
 *
 */

public class QuotientValue<V extends Number> extends BaseOperator
{
  public final transient DefaultInputPort<V> numerator = new DefaultInputPort<V>(this)
  {
    @Override
    public void process(V tuple)
    {
      numerators.value += tuple.doubleValue();
    }
  };
  public final transient DefaultInputPort<V> denominator = new DefaultInputPort<V>(this)
  {
    @Override
    public void process(V tuple)
    {
      denominators.value += tuple.doubleValue();
    }
  };

  public final transient DefaultOutputPort<Double> quotient = new DefaultOutputPort<Double>(this);
  MutableDouble numerators = new MutableDouble(0.0);
  MutableDouble denominators = new MutableDouble(0.0);

  int mult_by = 1;

  public void setMult_by(int i)
  {
    mult_by = i;
  }

  @Override
  public void beginWindow()
  {
    numerators.value = 0.0;
    denominators.value = 0.0;
  }

  @Override
  public void endWindow()
  {
    if (denominators.value == 0) {
      return;
    }
    quotient.emit(new Double((numerators.value/denominators.value)*mult_by));
  }
}
