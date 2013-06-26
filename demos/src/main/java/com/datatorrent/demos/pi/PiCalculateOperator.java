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
package com.datatorrent.demos.pi;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This operator implements Monte Carlo estimation of pi. For points randomly distributed points on 
 * square circle. pi ~= Number of poiints i  circle/Total number of points.
 *  
 */
public class PiCalculateOperator extends BaseOperator
{
  private transient int x = -1;
  private transient int y = -1;
  private int base;
  private transient double pi;
  private long inArea = 0;
  private long totalArea = 0;
  public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple)
    {
      if (x == -1) {
        x = tuple;
      }
      else {
        y = tuple;
        if (x * x + y * y <= base) {
          inArea++;
        }
        totalArea++;
        x = y = -1;
      }
    }

  };
  public final transient DefaultOutputPort<Double> output = new DefaultOutputPort<Double>();

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
    pi = (double)inArea / totalArea * 4;
    output.emit(pi);
  }

  public void setBase(int num)
  {
    base = num;
  }

  public int getBase()
  {
    return base;
  }

}
