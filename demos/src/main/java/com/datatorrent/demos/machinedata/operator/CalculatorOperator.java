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
package com.datatorrent.contrib.machinedata.operator;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.contrib.machinedata.data.*;
import com.datatorrent.contrib.machinedata.util.DataTable;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * CalculatorOperator class.
 * </p>
 *
 * @since 0.3.5
 */
public class CalculatorOperator extends BaseOperator
{

  private final DataTable<MachineKey, ResourceType, List<Integer>> data = new DataTable<MachineKey, ResourceType, List<Integer>>();

  @Min(1)
  @Max(99)
  private int kthPercentile = 95; // kth percentile
  private boolean computePercentile;
  private boolean computeSD;
  private boolean computeMax;

  private int percentileThreshold = 80;
  private int sdThreshold = 70;
  private int maxThreshold = 99;

  private transient DateFormat dateFormat = new SimpleDateFormat();

  @InputPortFieldAnnotation(name = "dataPort")
  public final transient DefaultInputPort<MachineInfo> dataPort = new DefaultInputPort<MachineInfo>()
  {
    @Override
    public void process(MachineInfo tuple)
    {
      addDataToCache(tuple);
    }

    /**
     * Stream codec used for partitioning.
     */
    @Override
    public Class<? extends StreamCodec<MachineInfo>> getStreamCodec()
    {
      return MachineInfoStreamCodec.class;
    }
  };

  @OutputPortFieldAnnotation(name = "percentileOutputPort")
  public final transient DefaultOutputPort<KeyValPair<MachineKey, Map<ResourceType, Double>>> percentileOutputPort = new DefaultOutputPort<KeyValPair<MachineKey, Map<ResourceType, Double>>>();

  @OutputPortFieldAnnotation(name = "sdOutputPort")
  public final transient DefaultOutputPort<KeyValPair<MachineKey, Map<ResourceType, Double>>> sdOutputPort = new DefaultOutputPort<KeyValPair<MachineKey, Map<ResourceType, Double>>>();

  @OutputPortFieldAnnotation(name = "maxOutputPort")
  public final transient DefaultOutputPort<KeyValPair<MachineKey, Map<ResourceType, Integer>>> maxOutputPort = new DefaultOutputPort<KeyValPair<MachineKey, Map<ResourceType, Integer>>>();

  @OutputPortFieldAnnotation(name = "smtpOutputPort")
  public transient DefaultOutputPort<String> smtpAlert = new DefaultOutputPort<String>();

  private void addDataToCache(MachineInfo tuple)
  {
    MachineKey machineKey = tuple.getMachineKey();
    if (!data.containsRow(machineKey)) {
      data.put(machineKey, ResourceType.CPU, Lists.<Integer>newArrayList());
      data.put(machineKey, ResourceType.RAM, Lists.<Integer>newArrayList());
      data.put(machineKey, ResourceType.HDD, Lists.<Integer>newArrayList());
    }
    data.get(machineKey, ResourceType.CPU).add(tuple.getCpu());
    data.get(machineKey, ResourceType.RAM).add(tuple.getRam());
    data.get(machineKey, ResourceType.HDD).add(tuple.getHdd());
  }

  @Override
  public void endWindow()
  {

    if (computePercentile) {
      for (MachineKey machineKey : data.rowKeySet()) {
        Collections.sort(data.get(machineKey, ResourceType.CPU));
        Collections.sort(data.get(machineKey, ResourceType.RAM));
        Collections.sort(data.get(machineKey, ResourceType.HDD));

        Map<ResourceType, Double> percentileData = Maps.newHashMap();
        percentileData.put(ResourceType.CPU, getKthPercentile(data.get(machineKey, ResourceType.CPU)));
        percentileData.put(ResourceType.RAM, getKthPercentile(data.get(machineKey, ResourceType.RAM)));
        percentileData.put(ResourceType.HDD, getKthPercentile(data.get(machineKey, ResourceType.HDD)));
        percentileOutputPort.emit(new KeyValPair<MachineKey, Map<ResourceType, Double>>(machineKey, percentileData));

        for (ResourceType resourceType : percentileData.keySet()) {
          double percentileValue = percentileData.get(resourceType);
          if (percentileValue > percentileThreshold) {
            emitAlert(resourceType, machineKey, percentileValue, "Percentile");
          }
        }
      }
    }
    if (computeSD) {
      for (MachineKey machineKey : data.rowKeySet()) {

        Map<ResourceType, Double> sdData = Maps.newHashMap();

        for (ResourceType resourceType : ResourceType.values()) {
          sdData.put(resourceType, getSD(data.get(machineKey, resourceType)));
        }
        sdOutputPort.emit(new KeyValPair<MachineKey, Map<ResourceType, Double>>(machineKey, sdData));

        for (ResourceType resourceType : sdData.keySet()) {
          double sdValue = sdData.get(resourceType);
          if (sdValue > sdThreshold) {
            emitAlert(resourceType, machineKey, sdValue, "SD");
          }
        }
      }
    }
    if (computeMax) {
      for (MachineKey machineKey : data.rowKeySet()) {

        Map<ResourceType, Integer> maxData = Maps.newHashMap();
        maxData.put(ResourceType.CPU, Collections.max(data.get(machineKey, ResourceType.CPU)));
        maxData.put(ResourceType.RAM, Collections.max(data.get(machineKey, ResourceType.RAM)));
        maxData.put(ResourceType.HDD, Collections.max(data.get(machineKey, ResourceType.HDD)));

        maxOutputPort.emit(new KeyValPair<MachineKey, Map<ResourceType, Integer>>(machineKey, maxData));

        for (ResourceType resourceType : maxData.keySet()) {
          double sdValue = maxData.get(resourceType).doubleValue();
          if (sdValue > maxThreshold) {
            emitAlert(resourceType, machineKey, sdValue, "Max");
          }
        }
      }
    }
    data.clear();
  }

  private void emitAlert(ResourceType type, MachineKey machineKey, double alertVal, String prefix)
  {
    BigDecimal decimalVal = new BigDecimal(alertVal);
    decimalVal = decimalVal.setScale(2, BigDecimal.ROUND_HALF_UP);
    String alertTime = machineKey.getDay() + machineKey.getTimeKey();
    smtpAlert.emit(prefix + "-" + type.toString().toUpperCase() + " alert at " + alertTime + " " + type + " usage breached current usage: " + decimalVal.doubleValue() + "% threshold: " + percentileThreshold + "%\n\n" + machineKey);
  }

  private double getKthPercentile(List<Integer> sorted)
  {

    double val = (kthPercentile * sorted.size()) / 100.0;
    if (val == (int) val) {
      // Whole number
      int idx = (int) val - 1;
      return (sorted.get(idx) + sorted.get(idx + 1)) / 2.0;
    }
    else {
      int idx = (int) Math.round(val) - 1;
      return sorted.get(idx);
    }
  }

  private double getSD(List<Integer> data)
  {
    int sum = 0;
    for (int i : data) {
      sum += i;
    }
    double avg = sum / (data.size() * 1.0);
    double sd = 0;
    for (Integer point : data) {
      sd += Math.pow(point - avg, 2);
    }
    return Math.sqrt(sd);
  }

  /**
   * @param kVal the percentile which will be emitted by this operator
   */
  public void setKthPercentile(int kVal)
  {
    this.kthPercentile = kVal;
  }

  /**
   * @param doCompute when true percentile will be computed
   */
  public void setComputePercentile(boolean doCompute)
  {
    this.computePercentile = doCompute;
  }

  /**
   * @param doCompute when true standard deviation will be computed
   */
  public void setComputeSD(boolean doCompute)
  {
    this.computeSD = doCompute;
  }

  /**
   * @param doCompute when true max will be computed
   */
  public void setComputeMax(boolean doCompute)
  {
    this.computeMax = doCompute;
  }

  /**
   * @param threshold for percentile when breached will cause alert
   */
  public void setPercentileThreshold(int threshold)
  {
    this.percentileThreshold = threshold;
  }

  /**
   * @param threshold for standard deviation when breached will cause alert
   */
  public void setSDThreshold(int threshold)
  {
    this.sdThreshold = threshold;
  }

  /**
   * @param threshold for Max when breached will cause alert
   */
  public void setMaxThreshold(int threshold)
  {
    this.maxThreshold = threshold;
  }

  public static class MachineInfoStreamCodec extends KryoSerializableStreamCodec<MachineInfo>
  {
    public MachineInfoStreamCodec()
    {
      super();
    }

    @Override
    public int getPartition(MachineInfo o)
    {
      return Objects.hashCode(o.getMachineKey().getCustomer(), o.getMachineKey().getOs(), o.getMachineKey().getProduct(), o.getMachineKey().getSoftware1(), o.getMachineKey().getSoftware2(), o.getMachineKey().getSoftware3());
    }
  }
}
