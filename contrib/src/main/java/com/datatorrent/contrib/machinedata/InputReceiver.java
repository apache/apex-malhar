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
public class InputReceiver extends BaseOperator implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(InputReceiver.class);

  public transient DefaultOutputPort<MachineInfo> outputInline = new DefaultOutputPort<MachineInfo>();
  public transient DefaultOutputPort<MachineInfo> output = new DefaultOutputPort<MachineInfo>();

  // TODO: why are there so many different random variables? why can't we use one instance
  private final Random randomGen = new Random();

  private int customerMin = 1;
  private int customerMax = 5;
  private int productMin = 4;
  private int productMax = 6;
  private int osMin = 10;
  private int osMax = 12;
  private int software1Min = 10;
  private int software1Max = 12;
  private int software2Min = 12;
  private int software2Max = 14;
  private int software3Min = 4;
  private int software3Max = 6;

  private int deviceIdMin = 1;
  private int deviceIdMax = 5;

  // private int tupleBlastSize = 50;
  private int tupleBlastSize = 1001;

  private int cpuThreshold = 70;
  private int ramThreshold = 70;
  private int hddThreshold = 90;
  private int operatorId;
  private long windowId = 1;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    operatorId = context.getId();
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.windowId = windowId;
  }

  @Override
  public void emitTuples()
  {
    int count = 0;

    while (count < tupleBlastSize) {
      randomGen.setSeed(System.currentTimeMillis());
      Calendar calendar = Calendar.getInstance();

      int customerVal = genCustomerId();
      int productVal = genProductVer();
      int osVal = genOsVer();
      int software1Val = genSoftware1Ver();
      int software2Val = genSoftware2Ver();
      int software3Val = genSoftware3Ver();
      int deviceIdVal = genDeviceId();

      int cpuVal = genCpu();
      int ramVal = genRam();
      int hddVal = genHdd();

      MachineKey machineKey = new MachineKey(calendar, MachineKey.TIMESPEC_MINUTE_SPEC);

      machineKey.setCustomer(customerVal);
      machineKey.setProduct(productVal);
      machineKey.setOs(osVal);
      machineKey.setDeviceId(deviceIdVal);
      machineKey.setSoftware1(software1Val);
      machineKey.setSoftware2(software2Val);
      machineKey.setSoftware3(software3Val);
      MachineInfo machineInfo = new MachineInfo();
      machineInfo.setMachineKey(machineKey);
      machineInfo.setCpu(cpuVal);
      machineInfo.setRam(ramVal);
      machineInfo.setHdd(hddVal);

      outputInline.emit(machineInfo);

      count++;
    }
  }

  public int genCustomerId()
  {
    int range = customerMax - customerMin + 1;
    return customerMin + randomGen.nextInt(range);
  }

  public int genProductVer()
  {
    int range = productMax - productMin + 1;
    return productMin + randomGen.nextInt(range);
  }

  public int genOsVer()
  {
    int range = osMax - osMin + 1;
    return osMin + randomGen.nextInt(range);
  }

  public int genSoftware3Ver()
  {
    int range = software3Max - software3Min + 1;
    return software3Min + randomGen.nextInt(range);
  }

  public int genDeviceId()
  {
    int range = deviceIdMax - deviceIdMin + 1;
    return deviceIdMin + randomGen.nextInt(range);
  }

  public int genSoftware1Ver()
  {
    int range = software1Max - software1Min + 1;
    return software1Min + randomGen.nextInt(range);
  }

  public int genSoftware2Ver()
  {
    int range = software2Max - software2Min + 1;
    return software2Min + randomGen.nextInt(range);
  }

  public int genCpu()
  {
    Calendar cal = Calendar.getInstance();
    int minute = cal.get(Calendar.MINUTE);    
    int second = cal.get(Calendar.SECOND);
    int range = minute/2 + 19;
    if (minute == 0) {
      return (30 + randomGen.nextInt(range) +(minute % 7) -(second % 11));
    } else {
      return (randomGen.nextInt(range) + (minute % 19) + (second % 11));
    }
  }

  public int genRam()
  {
    Calendar cal = Calendar.getInstance();

    int minute = cal.get(Calendar.MINUTE);
    int second = cal.get(Calendar.SECOND);
    int range = minute+1;
    if (minute / 23 == 0) {
      return (20 + randomGen.nextInt(range) + (minute % 5) - (second%11));
    } else {
      return (randomGen.nextInt(range) + (minute % 17) + (second%11));
    }
  }

  public int genHdd()
  {
    Calendar cal = Calendar.getInstance();

    int minute = cal.get(Calendar.MINUTE);
    int second = cal.get(Calendar.SECOND);
    int range = minute/2+1;
    if (minute / 37 == 0) {
      return (25+randomGen.nextInt(range) - minute % 7 - second%11);
    } else {
      return (randomGen.nextInt(range) + minute % 23 + second%11);
    }
  }

  public int getCpuThreshold()
  {
    return cpuThreshold;
  }

  public void setCpuThreshold(int cpuThreshold)
  {
    this.cpuThreshold = cpuThreshold;
  }

  public int getRamThreshold()
  {
    return ramThreshold;
  }

  public void setRamThreshold(int ramThreshold)
  {
    this.ramThreshold = ramThreshold;
  }

  public int getHddThreshold()
  {
    return hddThreshold;
  }

  public void setHddThreshold(int hddThreshold)
  {
    this.hddThreshold = hddThreshold;
  }
}
