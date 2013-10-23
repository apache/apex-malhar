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
  private final Random randomCustomerId = new Random();
  private final Random randomProductVer = new Random();
  private final Random randomOSVer = new Random();
  private final Random randomSoftware3Ver = new Random();
  private final Random randomSoftware1Ver = new Random();
  private final Random randomSoftware2Ver = new Random();
  private final Random randomCpu = new Random();
  private final Random randomRam = new Random();
  private final Random randomHdd = new Random();
  private final Random randomDeviceId = new Random();

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
  private int cpuMin = 10;
  private int cpuMax = 60;
  private int ramMin = 10;
  private int ramMax = 60;
  private int hddMin = 10;
  private int hddMax = 60;
  private static int roundrobin = 3;

  private int deviceIdMin = 1;
  private int deviceIdMax = 5;

  // private int tupleBlastSize = 50;
  private int tupleBlastSize = 1000;

  
  private int cpuThreshold = 70;
  private int ramThreshold = 70;
  private int hddThreshold = 90;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void emitTuples()
  {
    int count = 0;
    cpuMax = cpuMax - 2;
    ramMax = ramMax - 4;
    hddMin = hddMin + 5;
    while (count < tupleBlastSize) {
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
    return customerMin + randomCustomerId.nextInt(range);
  }

  public int genProductVer()
  {
    int range = productMax - productMin + 1;
    return productMin + randomProductVer.nextInt(range);
  }

  public int genOsVer()
  {
    int range = osMax - osMin + 1;
    return osMin + randomOSVer.nextInt(range);
  }

  public int genSoftware3Ver()
  {
    int range = software3Max - software3Min + 1;
    return software3Min + randomSoftware3Ver.nextInt(range);
  }

  public int genDeviceId()
  {
    int range = deviceIdMax - deviceIdMin + 1;
    return deviceIdMin + randomDeviceId.nextInt(range);
  }

  public int genSoftware1Ver()
  {
    int range = software1Max - software1Min + 1;
    return software1Min + randomSoftware1Ver.nextInt(range);
  }

  public int genSoftware2Ver()
  {
    int range = software2Max - software2Min + 1;
    return software2Min + randomSoftware2Ver.nextInt(range);
  }

  public int genCpu()
  {
    if (cpuMax <= (cpuMin + 10)) {
      cpuMax = 30 + (10)*(roundrobin%3);
      roundrobin = (roundrobin+1)%3;
      return cpuMax;
    }
    // cpuMax = cpuMax-2;
    int range = cpuMax - cpuMin + 1;
    cpuMax = cpuMin + randomCpu.nextInt(range);
    return cpuMax;
  }

  public int genRam()
  {
    if (ramMax <= (ramMin + 10)) {
      ramMax = 30 + (10)*(roundrobin%3);
      roundrobin = (roundrobin+1)%3;
    }
    // ramMax = ramMax-5;
    int range = ramMax - ramMin + 1;
    ramMax = ramMax - (randomRam.nextInt(range)) / 5;
    return ramMax;
  }

  public int genHdd()
  {
    if (hddMin >= (hddMax - 10)) {
      hddMin =  30 - (30)*(roundrobin%3);
      roundrobin = (roundrobin+1)%3;
      return hddMin;
    }
    // hddMax =hddMax- 8;
    int range = hddMax - hddMin + 1;
    hddMin = hddMin + (randomHdd.nextInt(range)) / 2;
    return hddMin;
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
