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
package org.apache.apex.examples.machinedata;

import org.apache.apex.examples.machinedata.data.MachineInfo;
import org.apache.apex.examples.machinedata.data.MachineKey;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;


/**
 * <p>
 * Information tuple generator with randomness.
 * </p>
 *
 * @since 0.3.5
 */
@SuppressWarnings("unused")
public class DimensionGenerator extends BaseOperator
{
  public transient DefaultOutputPort<MachineInfo> outputInline = new DefaultOutputPort<>();
  public transient DefaultOutputPort<MachineInfo> output = new DefaultOutputPort<>();
  private int threshold = 90;

  public final transient DefaultInputPort<MachineInfo> inputPort = new DefaultInputPort<MachineInfo>()
  {

    @Override
    public void process(MachineInfo tuple)
    {
      emitDimensions(tuple);
    }

  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  /**
   * This returns the threshold value set
   * @return
   */
  public int getThreshold()
  {
    return threshold;
  }

  /**
   * This function sets the threshold value. This value is used to check the maximum value for cpu/ram/hdd
   * @param threshold
   */
  public void setThreshold(int threshold)
  {
    this.threshold = threshold;
  }

  /**
   * This function takes in the tuple from upstream operator and generates tuples with different dimension combinations
   *
   * @param tuple
   */
  private void emitDimensions(MachineInfo tuple)
  {
    MachineKey tupleKey = tuple.getMachineKey();

    for (int i = 0; i < 64; i++) {
      MachineKey machineKey = new MachineKey(tupleKey.getTimeKey(),tupleKey.getDay());
      if ((i & 1) != 0) {
        machineKey.setCustomer(tupleKey.getCustomer());
      }
      if ((i & 2) != 0) {
        machineKey.setProduct(tupleKey.getProduct());
      }
      if ((i & 4) != 0) {
        machineKey.setOs(tupleKey.getOs());
      }
      if ((i & 8) != 0) {
        machineKey.setDeviceId(tupleKey.getDeviceId());
      }
      if ((i & 16) != 0) {
        machineKey.setSoftware1(tupleKey.getSoftware1());
      }
      if ((i & 32) != 0) {
        machineKey.setSoftware2(tupleKey.getSoftware2());
      }

      int cpu = tuple.getCpu();
      int ram = tuple.getRam();
      int hdd = tuple.getHdd();
      MachineInfo machineInfo = new MachineInfo();
      machineInfo.setMachineKey(machineKey);
      machineInfo.setCpu((cpu < threshold) ? cpu : threshold);
      machineInfo.setRam((ram < threshold) ? ram : threshold);
      machineInfo.setHdd((hdd < threshold) ? hdd : threshold);
      outputInline.emit(machineInfo);
      output.emit(machineInfo);
    }
  }

}
