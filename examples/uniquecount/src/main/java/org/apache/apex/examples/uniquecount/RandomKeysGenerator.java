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
package org.apache.apex.examples.uniquecount;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.apex.malhar.lib.util.KeyHashValPair;
import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/*
    Generate random keys.
 */
/**
 * <p>RandomKeysGenerator class.</p>
 *
 * @since 1.0.2
 */
public class RandomKeysGenerator implements InputOperator
{

  protected int numKeys = 100;
  protected int tupleBlast = 15000;
  protected long sleepTime = 0;
  protected Map<Integer, MutableInt> history = new HashMap<Integer, MutableInt>();
  private Random random = new Random();
  private Date date = new Date();
  private long start;

  @OutputPortFieldAnnotation(optional = false)
  public transient DefaultOutputPort<Integer> outPort = new DefaultOutputPort<Integer>();

  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort<KeyHashValPair<Integer, Integer>> verificationPort =
      new DefaultOutputPort<KeyHashValPair<Integer, Integer>>();

  @Override
  public void emitTuples()
  {
    for (int i = 0; i < tupleBlast; i++) {
      int key = random.nextInt(numKeys);
      outPort.emit(key);


      if (verificationPort.isConnected()) {
        // maintain history for later verification.
        MutableInt count = history.get(key);
        if (count == null) {
          count = new MutableInt(0);
          history.put(key, count);
        }
        count.increment();
      }

    }
    try {
      if (sleepTime != 0) {
        Thread.sleep(sleepTime);
      }
    } catch (Exception ex) {
      // Ignore.
    }
  }

  public RandomKeysGenerator()
  {
    start = date.getTime();
  }

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {

    if (verificationPort.isConnected()) {
      for (Map.Entry<Integer, MutableInt> e : history.entrySet()) {
        verificationPort.emit(new KeyHashValPair<Integer, Integer>(e.getKey(), e.getValue().toInteger()));
      }
      history.clear();
    }

  }

  @Override
  public void setup(Context.OperatorContext operatorContext)
  {

  }

  @Override
  public void teardown()
  {

  }

  public int getNumKeys()
  {
    return numKeys;
  }

  public void setNumKeys(int numKeys)
  {
    this.numKeys = numKeys;
  }

  public int getTupleBlast()
  {
    return tupleBlast;
  }

  public void setTupleBlast(int tupleBlast)
  {
    this.tupleBlast = tupleBlast;
  }

  public long getSleepTime()
  {
    return sleepTime;
  }

  public void setSleepTime(long sleepTime)
  {
    this.sleepTime = sleepTime;
  }
}
