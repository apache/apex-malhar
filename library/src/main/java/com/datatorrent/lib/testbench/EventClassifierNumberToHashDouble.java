/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.testbench;

import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;

import java.util.HashMap;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of BaseOperator that creates a load with pair of keys by taking in an input stream event and adding to incoming keys
 * to create a new tuple of Hashmap &lt;String,Double&gt; that is emitted on output port data.
 * <p>
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
 * @displayName Event Classifier Number To HashDouble
 * @category Testbench
 * @tags number, classifier
 * @since 0.3.2
 */
public class EventClassifierNumberToHashDouble<K extends Number> extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(EventClassifierNumberToHashDouble.class);
  public final transient DefaultInputPort<K> event = new DefaultInputPort<K>()
  {
    @Override
    public void process(K tuple)
    {
      double val = tuple.doubleValue();
      HashMap<String, Double> otuple = new HashMap<String, Double>(1);
      otuple.put(keys[seed], val);
      data.emit(otuple);
      seed++;
      if (seed >= seed_size) {
        seed = 0;
      }
    }
  };
  
  /**
   * Output data port that emits a hashmap of &lt;string,double&gt;.
   */
  public final transient DefaultOutputPort<HashMap<String, Double>> data = new DefaultOutputPort<HashMap<String, Double>>();

  @NotNull
  String key = "";
  int s_start = 0;
  int s_end = 0;
  int seed = 0;
  int seed_size = 1;

  String [] keys = null;

  /**
   * setup before dag is run (pre-runtime, and post compile time)
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    seed_size = s_start - s_end;
    if (seed_size < 0) {
      seed_size = s_end - s_start;
    }
    seed_size++;
    seed = 0;
    keys = new String[seed_size];
    if (s_start < s_end) {
      for (int i = s_start; i <= s_end; i++) {
        Integer ival = i;
        keys[i] = getKey() + ival.toString();
      }
    }
    else {
      for (int i = s_end; i <= s_start; i++) {
        Integer ival = i;
        keys[i] = getKey() + ival.toString();
      }
    }
  }


  /**
   * getter function for key
   * @return key
   */
  public String getKey()
  {
    return key;
  }

  /**
   * getter function for seed start
   * @return seed start
   */
  public int getSeedstart()
  {
    return s_start;
  }

  /**
   * getter function for seed end
   * @return seed end
   */
  public int getSeedend()
  {
    return s_end;
  }

  /**
   * Use this key to generate new keys.
   * @param i key is set to i
   */
  public void setKey(String i)
  {
    logger.debug("In setter of key");
    key = i;
  }

  /**
   * setter function for seed start
   * @param i seed start is set to i
   */
  public void setSeedstart(int i)
  {
    s_start = i;
  }

  /**
   * setter function for seed end
   * @param i seed end is set to i
   */
  public void setSeedend(int i)
  {
    s_end = i;
  }
}
