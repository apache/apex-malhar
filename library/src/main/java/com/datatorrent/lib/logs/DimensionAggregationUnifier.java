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
package com.datatorrent.lib.logs;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableDouble;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * <p>DimensionAggregationUnifier class.</p>
 *
 * @since 0.9.0
 */
public class DimensionAggregationUnifier implements Operator
{

  private Map<String, Map<String, MutableDouble>> dataMap = new HashMap<String, Map<String, MutableDouble>>();

  public final transient DefaultOutputPort<Map<String, DimensionObject<String>>> output = new DefaultOutputPort<Map<String, DimensionObject<String>>>();
  public final transient DefaultInputPort<Map<String, DimensionObject<String>>> input = new DefaultInputPort<Map<String, DimensionObject<String>>>() {

    @Override
    public void process(Map<String, DimensionObject<String>> tuple)
    {
      for (Map.Entry<String, DimensionObject<String>> e : tuple.entrySet()) {
        Map<String, MutableDouble> obj = dataMap.get(e.getKey());
        DimensionObject<String> eObj = e.getValue();
        if (obj == null) {
          obj = new HashMap<String, MutableDouble>();
          obj.put(eObj.getVal(), new MutableDouble(eObj.getCount()));
          dataMap.put(e.getKey(), obj);
        } else {
          MutableDouble n = obj.get(eObj.getVal());
          if (n == null) {
            obj.put(eObj.getVal(), new MutableDouble(eObj.getCount()));
          } else {
            n.add(eObj.getCount());
          }
        }
      }

    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void beginWindow(long windowId)
  {
    dataMap = new HashMap<String, Map<String, MutableDouble>>();
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<String, Map<String, MutableDouble>> e : dataMap.entrySet()) {
      for (Map.Entry<String, MutableDouble> dimensionValObj : e.getValue().entrySet()) {
        Map<String, DimensionObject<String>> outputData = new HashMap<String, DimensionObject<String>>();
        outputData.put(e.getKey(), new DimensionObject<String>(dimensionValObj.getValue(), dimensionValObj.getKey()));
        output.emit(outputData);
      }
    }
    dataMap.clear();
  }

}
