/*
* Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.datatorrent.contrib.machinedata;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.contrib.machinedata.data.MachineInfo;
import com.datatorrent.contrib.machinedata.data.MachineKey;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.contrib.machinedata.data.AverageData;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
* <p>AlertGeneratorOperator class.</p>
*
* @since 0.3.5
*/
public class AlertGeneratorOperator extends BaseOperator implements InputOperator
{
  public transient DefaultOutputPort<KeyValPair<MachineKey, Map<String, AverageData>>> alertPort
           = new DefaultOutputPort<KeyValPair<MachineKey, Map<String, AverageData>>>();

    public transient DefaultOutputPort<MachineInfo> alertMachineInfoPort
            = new DefaultOutputPort<MachineInfo>();

  private KeyValPair<MachineKey, Map<String, AverageData>> alertPair = null;
  private static DateFormat minuteDateFormat = new SimpleDateFormat("HHmm");
  private static Calendar calendar = Calendar.getInstance();

  @Override
  public void emitTuples()
  {
  }

  @Override
  public void endWindow()
  {
    if (alertPair != null) {
       
      Date date = calendar.getTime();
      String minute = minuteDateFormat.format(date);
      int day = calendar.get(Calendar.DAY_OF_MONTH);
      alertPair.getKey().setTimeKey(minute);
      alertPair.getKey().setDay(day);
      alertPort.emit(alertPair);
    }
  }

  public void setAlert(String alertKey) {
    String[] tokens = alertKey.split(",");
    if (tokens.length == 2) {
      Date date = calendar.getTime();
      String minute = minuteDateFormat.format(date);
      int day = calendar.get(Calendar.DAY_OF_MONTH);
      String key = tokens[0];
      String dim = tokens[1];
      Integer idim = Integer.parseInt(dim);
      MachineKey machineKey = new MachineKey(minute,day);
      parseMachineKey(key, machineKey);

      int cpu = 50, ram = 50, hdd = 50;
      int cpucount = 1, ramcount = 1, hddcount = 1;
      if (idim == 1) { cpu = 90; cpucount = 100000000; }
      if (idim == 2) { ram = 90; ramcount = 100000000; }
      if (idim == 3) { hdd = 90; hddcount = 100000000; }

      Map<String, AverageData> averageMap = new HashMap<String, AverageData>();
      AverageData cpuAverageData = new AverageData(((long)cpu*cpucount), cpucount);
      averageMap.put("cpu", cpuAverageData);
      AverageData ramAverageData = new AverageData(((long)ram*ramcount), ramcount);
      averageMap.put("ram", ramAverageData);
      AverageData hddAverageData = new AverageData(((long)hdd*hddcount), hddcount);
      averageMap.put("hdd", hddAverageData);
      alertPair = new KeyValPair<MachineKey, Map<String, AverageData>>(machineKey, averageMap);
      alertPort.emit(alertPair);
    }
  }

  public void setAlertOff(String value) {
    alertPair = null;
  }

  private void parseMachineKey(String key, MachineKey machineKey) {
    String[] tokens = key.split("\\|");
    for (int i=0; i < tokens.length; ++i) {
      String token = tokens[i];
      String[] dimTokens = token.split(":");
      if (dimTokens.length == 2) {
        String dimKey = dimTokens[0];
        String dimVal = dimTokens[1];
        Integer idimVal = Integer.parseInt(dimVal);
        if (dimKey.equals("0")) {
          machineKey.setCustomer(idimVal);
        } else if (dimKey.equals("1")) {
          machineKey.setProduct(idimVal);
        } else if (dimKey.equals("2")) {
          machineKey.setOs(idimVal);
        } else if (dimKey.equals("3")) {
          machineKey.setSoftware1(idimVal);
        } else if (dimKey.equals("4")) {
          machineKey.setSoftware2(idimVal);
        } else if (dimKey.equals("5")) {
          machineKey.setSoftware3(idimVal);
        }
      }
    }
  }

}