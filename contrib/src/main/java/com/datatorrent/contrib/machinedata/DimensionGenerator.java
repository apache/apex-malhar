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
  public transient DefaultOutputPort<String> smtpAlert = new DefaultOutputPort<String>();
  public transient DefaultOutputPort<KeyValPair<AlertKey, Map<String, Integer>>> alert = new DefaultOutputPort<KeyValPair<AlertKey, Map<String, Integer>>>();

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

  private void emitDimensions(MachineInfo tuple)
  {    
      Calendar calendar = Calendar.getInstance();
      MachineKey tupleKey = tuple.getMachineKey();

      for (int i = 0; i < 64; i++) {
        MachineKey machineKey = new MachineKey(calendar, MachineKey.TIMESPEC_MINUTE_SPEC);    
        if ((i & 1) != 0)
          machineKey.setCustomer(tupleKey.getCustomer());
        if ((i & 2) != 0)
          machineKey.setProduct(tupleKey.getProduct());
        if ((i & 4) != 0)
          machineKey.setOs(tupleKey.getOs());
        if ((i & 8) != 0)
          machineKey.setDeviceId(tupleKey.getDeviceId());
        if ((i & 16) != 0)
          machineKey.setSoftware1(tupleKey.getSoftware1());
        if ((i & 32) != 0)
          machineKey.setSoftware2(tupleKey.getSoftware2());
        MachineInfo machineInfo = new MachineInfo();
        machineInfo.setMachineKey(machineKey);
        machineInfo.setCpu(tuple.getCpu());
        machineInfo.setRam(tuple.getRam());
        machineInfo.setHdd(tuple.getHdd());
        outputInline.emit(machineInfo);
        output.emit(machineInfo);
      }
  }

  

}
