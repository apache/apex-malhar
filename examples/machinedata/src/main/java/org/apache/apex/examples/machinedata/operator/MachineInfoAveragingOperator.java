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

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.apex.examples.machinedata.data.AverageData;
import org.apache.apex.examples.machinedata.data.MachineInfo;
import org.apache.apex.examples.machinedata.data.MachineKey;
import org.apache.apex.malhar.lib.util.KeyHashValPair;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.google.common.collect.Maps;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * This class calculates the average for various resources across different devices for a given key
 * <p>MachineInfoAveragingOperator class.</p>
 *
 * @since 0.9.0
 */
@SuppressWarnings("unused")
public class MachineInfoAveragingOperator extends BaseOperator
{

  public static final String CPU = "cpu";
  public static final String RAM = "ram";
  public static final String HDD = "hdd";
  public static final String DAY = "day";

  private final transient Map<MachineKey, AverageData> dataMap = new HashMap<>();

  public final transient DefaultOutputPort<KeyValPair<MachineKey, Map<String, String>>> outputPort = new DefaultOutputPort<>();

  public transient DefaultOutputPort<String> smtpAlert = new DefaultOutputPort<>();

  private int threshold = 95;

  /**
   * Buffer all the tuples as is till end window gets called
   */
  public final transient DefaultInputPort<KeyHashValPair<MachineKey, AverageData>> inputPort = new DefaultInputPort<KeyHashValPair<MachineKey, AverageData>>()
  {

    @Override
    public void process(KeyHashValPair<MachineKey, AverageData> tuple)
    {
      addTuple(tuple);
    }
  };

  /**
   * This method returns the threshold value
   *
   * @return
   */
  public int getThreshold()
  {
    return threshold;
  }

  /**
   * This method sets the threshold value. If the average usage for any Resource is above this for a given key, then the alert is sent
   *
   * @param threshold the threshold value
   */
  public void setThreshold(int threshold)
  {
    this.threshold = threshold;
  }

  /**
   * This adds the given tuple to the dataMap
   *
   * @param tuple input tuple
   */
  private void addTuple(KeyHashValPair<MachineKey, AverageData> tuple)
  {
    MachineKey key = tuple.getKey();
    dataMap.put(key, tuple.getValue());
  }

  @Override
  public void endWindow()
  {
    for (Map.Entry<MachineKey, AverageData> entry : dataMap.entrySet()) {
      MachineKey key = entry.getKey();
      AverageData averageResultMap = entry.getValue();
      Map<String, String> averageResult = Maps.newHashMap();
      long count = averageResultMap.getCount();
      double average = averageResultMap.getCpu() / count;
      averageResult.put(CPU, average + "");
      emitAlert(average, CPU, key);
      average = averageResultMap.getHdd() / count;
      averageResult.put(HDD, average + "");
      emitAlert(average, HDD, key);
      average = averageResultMap.getRam() / count;
      averageResult.put(RAM, average + "");
      emitAlert(average, RAM, key);
      averageResult.put(DAY, key.getDay());
      outputPort.emit(new KeyValPair<>(key, averageResult));
    }
    dataMap.clear();
  }

  private void emitAlert(double average, String resourceType, MachineKey key)
  {
    if (average > threshold) {
      BigDecimal bd = new BigDecimal(average);
      bd = bd.setScale(2, BigDecimal.ROUND_HALF_UP);
      String stime = key.getDay() + key.getTimeKey();
      String skey = getKeyInfo(key);
      smtpAlert.emit(resourceType.toUpperCase() + " alert at " + stime + " " + resourceType + " usage breached current usage: " + bd.doubleValue() + "% threshold: " + threshold + "%\n\n" + skey);
    }
  }

  /**
   * This method is used to artificially generate alerts
   *
   * @param genAlert
   */
  public void setGenAlert(boolean genAlert)
  {
    Calendar calendar = Calendar.getInstance();
    long timestamp = System.currentTimeMillis();
    calendar.setTimeInMillis(timestamp);
    DateFormat minuteDateFormat = new SimpleDateFormat("HHmm");
    Date date = calendar.getTime();
    String timeKey = minuteDateFormat.format(date);
    DateFormat dayDateFormat = new SimpleDateFormat("dd");
    String day = dayDateFormat.format(date);

    MachineKey alertKey = new MachineKey(timeKey, day);
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

  /**
   * This method returns the String for a given MachineKey instance
   *
   * @param key MachineKey instance that needs to be converted to string
   * @return
   */
  private String getKeyInfo(MachineKey key)
  {
    StringBuilder sb = new StringBuilder();
    if (key instanceof MachineKey) {
      MachineKey mkey = (MachineKey)key;
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
