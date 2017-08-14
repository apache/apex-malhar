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
package org.apache.apex.examples.distributeddistinct;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.KeyHashValPair;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Generates random KeyValPairs and optionally, keeps track of the number of unique values per pair to emit to the
 * verification port.
 *
 * @since 1.0.4
 */
public class RandomKeyValGenerator implements InputOperator
{

  protected int numKeys = 50;
  protected int numVals = 1000;
  protected int tupleBlast = 1000;
  protected Map<Integer, Set<Integer>> valhistory = new HashMap<Integer, Set<Integer>>();
  private Random rand = new Random();
  private boolean once;
  private boolean clearHistory;
  @OutputPortFieldAnnotation(optional = false)
  public transient DefaultOutputPort<KeyValPair<Integer, Object>> outport = new DefaultOutputPort<KeyValPair<Integer, Object>>();

  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort<KeyValPair<Integer, Integer>> verport = new DefaultOutputPort<KeyValPair<Integer, Integer>>();

  /**
   * Ensures that the generator emits KeyValPairs once per window
   */
  @Override
  public void beginWindow(long l)
  {
    once = false;
  }

  /**
   * Emits the total count of unique values per key as KeyHashValPairs to the verification port
   */
  @Override
  public void endWindow()
  {
    if (verport.isConnected()) {
      for (Map.Entry<Integer, Set<Integer>> e : valhistory.entrySet()) {
        verport.emit(new KeyHashValPair<Integer, Integer>(e.getKey(), e.getValue().size()));
      }
    }
    if (clearHistory) {
      valhistory.clear();
    }
  }

  @Override
  public void setup(OperatorContext arg0)
  {

  }

  @Override
  public void teardown()
  {

  }

  /**
   * Emits random KeyValPairs and keeps track of the unique values per key.
   */
  @Override
  public void emitTuples()
  {
    if (!once) {
      int key;
      int val;
      for (int i = 0; i < tupleBlast; i++) {
        key = rand.nextInt(numKeys);
        val = rand.nextInt(numVals);
        outport.emit(new KeyValPair<Integer, Object>(key, val));
        if (verport.isConnected()) {
          Set<Integer> count = valhistory.get(key);
          if (count == null) {
            Set<Integer> tempset = new HashSet<Integer>();
            tempset.add(val);
            valhistory.put(key, tempset);
            LOG.debug("key {} val {}", key, tempset);
          } else if (!valhistory.get(key).contains(val)) {
            valhistory.get(key).add(val);
          }
        }
      }
      once = true;
    }
  }

  /**
   * @return the number of possible keys
   */
  public int getNumKeys()
  {
    return numKeys;
  }

  /**
   * Sets the number of possible keys to numKeys
   *
   * @param numKeys
   *          the new number of possible keys
   */
  public void setNumKeys(int numKeys)
  {
    this.numKeys = numKeys;
  }

  /**
   * Returns the number of possible values that can be emitted
   *
   * @return the number of possible values that can be emitted
   */
  public int getNumVals()
  {
    return numVals;
  }

  /**
   * Sets the number of possible values that can be emitted to numVals
   *
   * @param numVals
   *          the number of possible values that can be emitted
   */
  public void setNumVals(int numVals)
  {
    this.numVals = numVals;
  }

  /**
   * Sets the number of KeyValPairs to be emitted to tupleBlast
   *
   * @param tupleBlast
   *          the new number of KeyValPairs to be emitted
   */
  public void setTupleBlast(int tupleBlast)
  {
    this.tupleBlast = tupleBlast;
  }

  /**
   * tuple blast
   * @return
   */
  public int getTupleBlast()
  {
    return tupleBlast;
  }

  private static final Logger LOG = LoggerFactory.getLogger(RandomKeyValGenerator.class);
}
