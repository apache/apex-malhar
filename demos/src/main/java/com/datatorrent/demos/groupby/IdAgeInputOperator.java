/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.groupby;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;

import java.util.HashMap;
import java.util.Random;

/**
 *
 */
public class IdAgeInputOperator implements InputOperator
{
  public transient DefaultOutputPort<HashMap<String, Integer>>output = new DefaultOutputPort<HashMap<String, Integer>>();
  private Random random;
  private transient int interval;

  public void setInterval(int ms) {
    interval = ms;
  }

  @Override
  public void emitTuples()
  {
    Integer id = random.nextInt(10);

    Integer age = id+20;
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    map.put("id", id);
    map.put("age", age);
    output.emit(map);
    try {
      Thread.sleep(interval);
    }
    catch (InterruptedException ex) {
      System.out.println(ex.toString());
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    random = new Random(System.nanoTime());
  }

  @Override
  public void teardown()
  {
  }
}
