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
package com.datatorrent.lib.io;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import java.util.ArrayList;

/**
 * Reusable operator to collect any tuple.
 * Mainly used for testing.
 *
 */
public class TestTupleCollector<T> extends BaseOperator
{
  public ArrayList<T> collectedTuples = new ArrayList<T>();
  @InputPortFieldAnnotation(name = "input")
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      collectedTuples.add(tuple);
      count++;
    }
  };

  public long count = 0;

  public String firstTuple()
  {
    if (collectedTuples.isEmpty()){
      return null;
    }
    else {
      return collectedTuples.get(0).toString();
    }
  }

}
