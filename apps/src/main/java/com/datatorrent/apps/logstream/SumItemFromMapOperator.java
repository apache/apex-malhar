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
package com.datatorrent.apps.logstream;

import java.util.Map;

import javax.validation.constraints.NotNull;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Sums the value of the selected dimension from the map over application window
 *
 * @param <K> dimension to select from map
 * @param <V> value to be summed
 */
public class SumItemFromMapOperator<K, V> extends BaseOperator
{
  @NotNull
  private K sumDimension;
  private long sum = 0L;

  public void setSumDimension(K sumDimension)
  {
    this.sumDimension = sumDimension;
  }

  public final transient DefaultInputPort<Map<K, V>> mapInput = new DefaultInputPort<Map<K, V>>()
  {
    @Override
    public void process(Map<K, V> tuple)
    {
      V val = tuple.get(sumDimension);
      if (val != null) {
        sum += Long.valueOf((String)val);
      }
    }

  };
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  public void endWindow()
  {
    output.emit(String.valueOf(sum));
    sum = 0L;
  }

}
