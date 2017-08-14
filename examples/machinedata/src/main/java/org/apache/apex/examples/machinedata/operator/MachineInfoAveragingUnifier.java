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
package org.apache.apex.examples.machinedata.operator;

import java.util.HashMap;
import java.util.Map;

import org.apache.apex.examples.machinedata.data.AverageData;
import org.apache.apex.examples.machinedata.data.MachineKey;
import org.apache.apex.malhar.lib.util.KeyHashValPair;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;

/**
 * This class calculates the partial sum and count for a given key
 * <p>MachineInfoAveragingUnifier class.</p>
 *
 * @since 0.9.0
 */
public class MachineInfoAveragingUnifier implements Unifier<KeyHashValPair<MachineKey, AverageData>>
{

  private Map<MachineKey, AverageData> sums = new HashMap<>();
  public final transient DefaultOutputPort<KeyHashValPair<MachineKey, AverageData>> outputPort = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<MachineKey, AverageData> entry : sums.entrySet()) {
      outputPort.emit(new KeyHashValPair<>(entry.getKey(), entry.getValue()));
    }
    sums.clear();

  }

  @Override
  public void setup(OperatorContext arg0)
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void teardown()
  {
    // TODO Auto-generated method stub

  }

  @Override
  public void process(KeyHashValPair<MachineKey, AverageData> arg0)
  {
    MachineKey tupleKey = arg0.getKey();
    AverageData averageData = sums.get(tupleKey);
    AverageData tupleValue = arg0.getValue();
    if (averageData == null) {
      sums.put(tupleKey, tupleValue);
    } else {
      averageData.setCpu(averageData.getCpu() + tupleValue.getCpu());
      averageData.setRam(averageData.getRam() + tupleValue.getRam());
      averageData.setHdd(averageData.getHdd() + tupleValue.getHdd());
      averageData.setCount(averageData.getCount() + tupleValue.getCount());
    }
  }

}
