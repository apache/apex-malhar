/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.uniquecountdemo;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.util.KeyHashValPair;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.*;

/*
    Generate random keys.
 */
public class RandKeyGen implements InputOperator
{

  protected int numKeys = 100;
  protected int tuppleBlast = 15000;
  protected long sleepTime = 0;
  protected Map<Integer, MutableInt> history = new HashMap<Integer, MutableInt>();
  private Random random = new Random();
  private Date date = new Date();
  private long start;

  @OutputPortFieldAnnotation(name="keys", optional = false)
  public transient DefaultOutputPort<Integer> outPort = new DefaultOutputPort<Integer>();

  @Override
  public void emitTuples()
  {
    for (int i = 0; i < tuppleBlast; i++) {
      int key = random.nextInt(numKeys);
      outPort.emit(key);
    }
    try {
      if (sleepTime != 0)
        Thread.sleep(sleepTime);
    } catch (Exception ex) {

    }
  }

  public RandKeyGen()
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

  public int getTuppleBlast()
  {
    return tuppleBlast;
  }

  public void setTuppleBlast(int tuppleBlast)
  {
    this.tuppleBlast = tuppleBlast;
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

