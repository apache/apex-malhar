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
package com.datatorrent.contrib.machinedata;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.contrib.machinedata.data.MachineInfo;
import com.datatorrent.contrib.machinedata.data.MachineKey;
import com.datatorrent.lib.util.KeyValPair;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger logger = LoggerFactory.getLogger(DimensionGenerator.class);

  public transient DefaultOutputPort<MachineInfo> outputInline = new DefaultOutputPort<MachineInfo>();
  public transient DefaultOutputPort<MachineInfo> output = new DefaultOutputPort<MachineInfo>();
  private static final Random randomGen = new Random();
  private int threshold=90;
  
  public final transient DefaultInputPort<MachineInfo> inputPort = new DefaultInputPort<MachineInfo>() {

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
    Calendar calendar = Calendar.getInstance();
    MachineKey tupleKey = tuple.getMachineKey();
    int random = 0; // this is added to make the data more random for different dimension combinations

    for (int i = 0; i < 64; i++) {
      MachineKey machineKey = new MachineKey(tupleKey.getTimeKey(),tupleKey.getDay());    
      if ((i & 1) != 0) {
        machineKey.setCustomer(tupleKey.getCustomer());
        //random += machineKey.getCustomer();
      }
      if ((i & 2) != 0) {
        machineKey.setProduct(tupleKey.getProduct());
        //random += machineKey.getProduct();
      }
      if ((i & 4) != 0) {
        machineKey.setOs(tupleKey.getOs());
        //random += machineKey.getOs();
      }
      if ((i & 8) != 0) {
        machineKey.setDeviceId(tupleKey.getDeviceId());
        //random += machineKey.getDeviceId();
      }
      if ((i & 16) != 0) {
        machineKey.setSoftware1(tupleKey.getSoftware1());
        //random += machineKey.getSoftware1();
      }
      if ((i & 32) != 0) {
        machineKey.setSoftware2(tupleKey.getSoftware2());
        //random += machineKey.getSoftware2();
      }
      /*
      if (random > 0) {
        randomGen.setSeed(System.currentTimeMillis());
        random = randomGen.nextInt(random);
      }
      */
      int cpu = tuple.getCpu();
      int ram = tuple.getRam();
      int hdd = tuple.getHdd();
      MachineInfo machineInfo = new MachineInfo();
      machineInfo.setMachineKey(machineKey);
      machineInfo.setCpu((cpu < threshold)?cpu:threshold);
      machineInfo.setRam((ram < threshold)?ram:threshold);
      machineInfo.setHdd((hdd < threshold)?hdd:threshold);
      outputInline.emit(machineInfo);
      output.emit(machineInfo);
    }
  }

}
