/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import java.util.HashMap;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a in stream event and adds to incoming keys to create a new tuple that is emitted on output port data. The aim is to create a load with pair of keys<p>
 * <br>
 * Examples of pairs include<br>
 * publisher,advertizer<br>
 * automobile,model<br>
 * <br>
 * The keys to be inserted are given by the property <b>keys</b>. Users can choose to insert their
 * own values via property <b>values</b>. Insertion can be done as replacement, addition, multiply,
 * or append (append is not yet supported)<br>. For each incoming key users can provide an insertion
 * probability for the insert keys. This allows for randomization of the insert key choice<br><br>
 * <br>
 * <b>Tuple Schema</b>: Each tuple is HashMap<String, Double> on both the ports. Currently other schemas are not supported<br>
 * <b>Port Interface</b><br>
 * <b>data</b>: emits HashMap<String,Double><br>
 * <b>event</b>: expects HashMap<String,Double><br>
 * <br>
 * <b>Properties</b>:
 * None<br>
 * <br>
 * Compile time checks are:<br>
 * <b>keys</b> cannot be empty<br>
 * <b>values</b> if specified has to be comma separated doubles and their number must match the number of keys<br>
 * <b>weights</b> if specified the format has to be "key1:val1,val2,...,valn;key2:val1,val2,...,valn;...", where n has to be
 * number of keys in parameter <b>keys</b>. If not specified all weights are equal<br>
 * <br>
 * <br>
 * <b>Benchmarks</b>: This node has been benchmarked at over 5 million tuples/second in local/inline mode<br>
 *
 * @author amol
 */

public class EventClassifierNumberToHashDouble<K extends Number> extends BaseOperator
{
  public final transient DefaultInputPort<K> event = new DefaultInputPort<K>(this)
  {
    @Override
    public void process(K tuple)
    {
      double val = tuple.doubleValue();
      HashMap<String,Double> otuple = new HashMap<String,Double>(1);
      otuple.put(getKey(), val);
      data.emit(otuple);
    }
  };
  public final transient DefaultOutputPort<HashMap<String, Double>> data = new DefaultOutputPort<HashMap<String, Double>>(this);

  @NotNull
  String key = "";

  public String getKey()
  {
    return key;
  }

  public void setKey(String i)
  {
    key = i;
  }
}
