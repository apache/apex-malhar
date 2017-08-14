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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.examples.machinedata.data.MachineInfo;
import org.apache.apex.examples.machinedata.data.MachineKey;
import org.apache.apex.examples.machinedata.data.ResourceType;
import org.apache.apex.examples.machinedata.operator.CalculatorOperator;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.util.TimeBucketKey;

import com.google.common.collect.ImmutableList;

/**
 * @since 0.3.5
 */
public class CalculatorOperatorTest
{
  private static DateFormat minuteDateFormat = new SimpleDateFormat("HHmm");
  private static Logger LOG = LoggerFactory.getLogger(CalculatorOperatorTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  public void testNodeProcessing() throws Exception
  {
    CalculatorOperator calculatorOperator = new CalculatorOperator();
    calculatorOperator.setup(null);

    calculatorOperator.setComputePercentile(true);
    calculatorOperator.setComputeMax(true);
    calculatorOperator.setComputeSD(true);

    testPercentile(calculatorOperator);
  }

  public void testPercentile(CalculatorOperator oper)
  {

    CollectorTestSink sortSink = new CollectorTestSink();
    oper.percentileOutputPort.setSink(sortSink);
    oper.setKthPercentile(50);
    Calendar calendar = Calendar.getInstance();
    Date date = calendar.getTime();
    String timeKey = minuteDateFormat.format(date);
    String day = calendar.get(Calendar.DAY_OF_MONTH) + "";

    Integer vs = new Integer(1);
    MachineKey mk = new MachineKey(timeKey, day, vs, vs, vs, vs, vs, vs, vs);

    oper.beginWindow(0);

    MachineInfo info = new MachineInfo(mk, 1, 1, 1);
    oper.dataPort.process(info);

    info.setCpu(2);
    oper.dataPort.process(info);

    info.setCpu(3);
    oper.dataPort.process(info);

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    for (Object o : sortSink.collectedTuples) {
      LOG.debug(o.toString());
      KeyValPair<TimeBucketKey, Map<ResourceType, Double>> keyValPair = (KeyValPair<TimeBucketKey, Map<ResourceType, Double>>)o;
      Assert.assertEquals("emitted value for 'cpu' was ", 2.0, keyValPair.getValue().get(ResourceType.CPU), 0);
      Assert.assertEquals("emitted value for 'hdd' was ", 1.0, keyValPair.getValue().get(ResourceType.HDD), 0);
      Assert.assertEquals("emitted value for 'ram' was ", 1.0, keyValPair.getValue().get(ResourceType.RAM), 0);

    }
    LOG.debug("Done percentile testing\n");

  }

  public void testStandarDeviation(CalculatorOperator oper)
  {
    CollectorTestSink sortSink = new CollectorTestSink();
    oper.sdOutputPort.setSink(sortSink);
    Calendar calendar = Calendar.getInstance();
    Date date = calendar.getTime();
    String timeKey = minuteDateFormat.format(date);
    String day = calendar.get(Calendar.DAY_OF_MONTH) + "";

    Integer vs = new Integer(1);
    MachineKey mk = new MachineKey(timeKey, day, vs, vs, vs, vs, vs, vs, vs);

    oper.beginWindow(0);

    MachineInfo info = new MachineInfo(mk, 1, 1, 1);
    oper.dataPort.process(info);

    info.setCpu(2);
    oper.dataPort.process(info);

    info.setCpu(3);
    oper.dataPort.process(info);

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    for (Object o : sortSink.collectedTuples) {
      LOG.debug(o.toString());
      KeyValPair<TimeBucketKey, Map<ResourceType, Double>> keyValPair = (KeyValPair<TimeBucketKey, Map<ResourceType, Double>>)o;
      Assert.assertEquals("emitted value for 'cpu' was ", getSD(ImmutableList.of(1, 2, 3)), keyValPair.getValue().get(ResourceType.CPU), 0);
      Assert.assertEquals("emitted value for 'hdd' was ", getSD(ImmutableList.of(1, 1, 1)), keyValPair.getValue().get(ResourceType.HDD), 0);
      Assert.assertEquals("emitted value for 'ram' was ", getSD(ImmutableList.of(1, 1, 1)), keyValPair.getValue().get(ResourceType.RAM), 0);

    }
    LOG.debug("Done sd testing\n");

  }

  private final double getSD(List<Integer> input)
  {
    int sum = 0;
    for (int i : input) {
      sum += i;
    }
    double avg = sum / (input.size() * 1.0);
    double sd = 0;
    for (Integer point : input) {
      sd += Math.pow(point - avg, 2);
    }
    return Math.sqrt(sd);
  }

  public void testMax(CalculatorOperator oper)
  {
    CollectorTestSink sortSink = new CollectorTestSink();
    oper.maxOutputPort.setSink(sortSink);
    Calendar calendar = Calendar.getInstance();
    Date date = calendar.getTime();
    String timeKey = minuteDateFormat.format(date);
    String day = calendar.get(Calendar.DAY_OF_MONTH) + "";

    Integer vs = new Integer(1);
    MachineKey mk = new MachineKey(timeKey, day, vs, vs, vs, vs, vs, vs, vs);

    oper.beginWindow(0);

    MachineInfo info = new MachineInfo(mk, 1, 1, 1);
    oper.dataPort.process(info);

    info.setCpu(2);
    oper.dataPort.process(info);

    info.setCpu(3);
    oper.dataPort.process(info);

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
    for (Object o : sortSink.collectedTuples) {
      LOG.debug(o.toString());
      KeyValPair<TimeBucketKey, Map<ResourceType, Double>> keyValPair = (KeyValPair<TimeBucketKey, Map<ResourceType, Double>>)o;
      Assert.assertEquals("emitted value for 'cpu' was ", 3, keyValPair.getValue().get(ResourceType.CPU), 0);
      Assert.assertEquals("emitted value for 'hdd' was ", 1, keyValPair.getValue().get(ResourceType.HDD), 0);
      Assert.assertEquals("emitted value for 'ram' was ", 1, keyValPair.getValue().get(ResourceType.RAM), 0);

    }
    LOG.debug("Done max testing\n");

  }
}
