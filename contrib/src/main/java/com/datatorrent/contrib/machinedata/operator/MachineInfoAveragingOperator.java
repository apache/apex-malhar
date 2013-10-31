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
package com.datatorrent.contrib.machinedata.operator;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.machinedata.AlertKey;
import com.datatorrent.contrib.machinedata.data.MachineInfo;
import com.datatorrent.contrib.machinedata.data.MachineKey;
import com.datatorrent.contrib.machinedata.data.AverageData;
import com.datatorrent.contrib.machinedata.data.ResourceType;
import com.datatorrent.lib.util.KeyHashValPair;
import com.datatorrent.lib.util.KeyValPair;
import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
*
*/
@SuppressWarnings("unused")
public class MachineInfoAveragingOperator extends BaseOperator
{

  private Map<MachineKey, List<Map<String, AverageData>>> dataMap = new HashMap<MachineKey, List<Map<String, AverageData>>>();

  public final transient DefaultOutputPort<KeyValPair<MachineKey, Map<ResourceType, Double>>> outputPort = new DefaultOutputPort<KeyValPair<MachineKey, Map<ResourceType, Double>>>();

  public transient DefaultOutputPort<String> smtpAlert = new DefaultOutputPort<String>();

  private int threshold = 95;

  private boolean genAlert;
  private transient DateFormat dateFormat = new SimpleDateFormat();

  /**
   * Buffer all the tuples as is till end window gets called
   */
  public final transient DefaultInputPort<KeyHashValPair<MachineKey, Map<String, AverageData>>> inputPort = new DefaultInputPort<KeyHashValPair<MachineKey, Map<String, AverageData>>>() {

    @Override
    public void process(KeyHashValPair<MachineKey, Map<String, AverageData>> tuple)
    {
      addTuple(tuple);
    }
  };

  public int getThreshold()
  {
    return threshold;
  }

  public void setThreshold(int threshold)
  {
    this.threshold = threshold;
  }

  private void addTuple(KeyHashValPair<MachineKey, Map<String, AverageData>> tuple)
  {
    MachineKey key = tuple.getKey();
    List<Map<String, AverageData>> list = dataMap.get(key);
    if (list == null) {
      list = new ArrayList<Map<String, AverageData>>();
      dataMap.put(key, list);
    }
    list.add(tuple.getValue());
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<MachineKey, List<Map<String, AverageData>>> entry : dataMap.entrySet()) {
      MachineKey key = entry.getKey();
      List<Map<String, AverageData>> list = entry.getValue();

      Map<ResourceType, AverageData> averageResultMap = Maps.newHashMap();

      for (Map<String, AverageData> map : list) {
        prepareAverageResult(map, ResourceType.CPU, averageResultMap);
        prepareAverageResult(map, ResourceType.RAM, averageResultMap);
        prepareAverageResult(map, ResourceType.HDD, averageResultMap);
      }
      Map<ResourceType, Double> averageResult = Maps.newHashMap();

      for (Map.Entry<ResourceType, AverageData> dataEntry : averageResultMap.entrySet()) {
        ResourceType resourceType = dataEntry.getKey();
        double average = dataEntry.getValue().getSum() / dataEntry.getValue().getCount();
        averageResult.put(resourceType, average);

        if (average > threshold) {
          BigDecimal bd = new BigDecimal(average);
          bd = bd.setScale(2, BigDecimal.ROUND_HALF_UP);
          String stime = dateFormat.format(key.getTime().getTime());
          String skey = getKeyInfo(key);

          smtpAlert.emit(resourceType.toString().toUpperCase() + " alert at " + stime + " " + resourceType + " usage breached current usage: " + bd.doubleValue() + "% threshold: " + threshold + "%\n\n" + skey);
        }
      }
      outputPort.emit(new KeyValPair<MachineKey, Map<ResourceType, Double>>(key, averageResult));
    }
    dataMap.clear();
  }

  private void prepareAverageResult(Map<String, AverageData> map, ResourceType valueKey, Map<ResourceType, AverageData> averageResultMap)
  {
    AverageData average = averageResultMap.get(valueKey);
    if (average == null) {
      average = new AverageData(map.get(valueKey.toString()).getSum(), map.get(valueKey.toString()).getCount());
      averageResultMap.put(valueKey, average);
    } else {
      average.setSum(average.getSum() + map.get(valueKey.toString()).getSum());
      average.setCount(average.getCount() + map.get(valueKey.toString()).getCount());
    }
  }

  public void setGenAlert(boolean genAlert)
  {
    Calendar calendar = Calendar.getInstance();
    long timestamp = System.currentTimeMillis();
    calendar.setTimeInMillis(timestamp);

    MachineKey alertKey = new MachineKey(calendar, MachineKey.TIMESPEC_MINUTE_SPEC);
    alertKey.setCustomer(1);
    alertKey.setProduct(5);
    alertKey.setOs(10);
    alertKey.setSoftware1(12);
    alertKey.setSoftware2(14);
    alertKey.setSoftware3(6);

    MachineInfo machineInfo = new MachineInfo();
    machineInfo.setMachineKey(alertKey);
    machineInfo.setCpu(threshold + 1);
    machineInfo.setRam(threshold + 1);
    machineInfo.setHdd(threshold + 1);

    smtpAlert.emit("CPU Alert: CPU Usage threshold (" + threshold + ") breached: current % usage: " + getKeyInfo(alertKey));
    smtpAlert.emit("RAM Alert: RAM Usage threshold (" + threshold + ") breached: current % usage: " + getKeyInfo(alertKey));
    smtpAlert.emit("HDD Alert: HDD Usage threshold (" + threshold + ") breached: current % usage: " + getKeyInfo(alertKey));
  }

  private String getKeyInfo(MachineKey key)
  {
    StringBuilder sb = new StringBuilder();
    if (key instanceof MachineKey) {
      MachineKey mkey = (MachineKey) key;
      Integer customer = mkey.getCustomer();
      if (customer != null) {
        sb.append("customer: " + customer + "\n");
      }
      Integer product = mkey.getProduct();
      if (product != null) {
        sb.append("product version: " + product + "\n");
      }
      Integer os = mkey.getOs();
      if (os != null) {
        sb.append("os version: " + os + "\n");
      }
      Integer software1 = mkey.getSoftware1();
      if (software1 != null) {
        sb.append("software1 version: " + software1 + "\n");
      }
      Integer software2 = mkey.getSoftware2();
      if (software2 != null) {
        sb.append("software2 version: " + software2 + "\n");
      }
      Integer software3 = mkey.getSoftware3();
      if (software3 != null) {
        sb.append("software3 version: " + software3 + "\n");
      }
    }
    return sb.toString();
  }
}
