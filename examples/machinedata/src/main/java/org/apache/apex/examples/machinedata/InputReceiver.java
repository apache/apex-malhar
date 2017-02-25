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
import java.util.Random;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.examples.machinedata.data.MachineInfo;
import org.apache.apex.examples.machinedata.data.MachineKey;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

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

  public transient DefaultOutputPort<MachineInfo> outputInline = new DefaultOutputPort<>();
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
  private int deviceIdMax = 50;

  private int tupleBlastSize = 1001;

  private static final DateFormat minuteDateFormat = new SimpleDateFormat("HHmm");
  private static final DateFormat dayDateFormat = new SimpleDateFormat("d");

  static {
    TimeZone tz = TimeZone.getTimeZone("GMT");
    minuteDateFormat.setTimeZone(tz);
    dayDateFormat.setTimeZone(tz);

  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
  }

  @Override
  public void emitTuples()
  {
    int count = 0;
    Calendar calendar = Calendar.getInstance();
    Date date = calendar.getTime();
    String timeKey = minuteDateFormat.format(date);
    String day = dayDateFormat.format(date);

    while (count < tupleBlastSize) {
      randomGen.setSeed(System.currentTimeMillis());

      int customerVal = genCustomerId();
      int productVal = genProductVer();
      int osVal = genOsVer();
      int software1Val = genSoftware1Ver();
      int software2Val = genSoftware2Ver();
      int software3Val = genSoftware3Ver();
      int deviceIdVal = genDeviceId();

      int cpuVal = genCpu(calendar);
      int ramVal = genRam(calendar);
      int hddVal = genHdd(calendar);

      MachineKey machineKey = new MachineKey(timeKey, day);

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

  private int genCustomerId()
  {
    int range = customerMax - customerMin + 1;
    return customerMin + randomGen.nextInt(range);
  }

  private int genProductVer()
  {
    int range = productMax - productMin + 1;
    return productMin + randomGen.nextInt(range);
  }

  private int genOsVer()
  {
    int range = osMax - osMin + 1;
    return osMin + randomGen.nextInt(range);
  }

  private int genSoftware3Ver()
  {
    int range = software3Max - software3Min + 1;
    return software3Min + randomGen.nextInt(range);
  }

  private int genDeviceId()
  {
    int range = deviceIdMax - deviceIdMin + 1;
    return deviceIdMin + randomGen.nextInt(range);
  }

  private int genSoftware1Ver()
  {
    int range = software1Max - software1Min + 1;
    return software1Min + randomGen.nextInt(range);
  }

  private int genSoftware2Ver()
  {
    int range = software2Max - software2Min + 1;
    return software2Min + randomGen.nextInt(range);
  }

  private int genCpu(Calendar cal)
  {
    int minute = cal.get(Calendar.MINUTE);
    int second;
    int range = minute / 2 + 19;
    if (minute / 17 == 0) {
      second = cal.get(Calendar.SECOND);
      return (30 + randomGen.nextInt(range) + (minute % 7) - (second % 11));
    } else if (minute / 47 == 0) {
      second = cal.get(Calendar.SECOND);
      return (7 + randomGen.nextInt(range) + (minute % 7) - (second % 7));
    } else {
      second = cal.get(Calendar.SECOND);
      return (randomGen.nextInt(range) + (minute % 19) + (second % 7));
    }
  }

  private int genRam(Calendar cal)
  {
    int minute = cal.get(Calendar.MINUTE);
    int second;
    int range = minute + 1;
    if (minute / 23 == 0) {
      second = cal.get(Calendar.SECOND);
      return (20 + randomGen.nextInt(range) + (minute % 5) - (second % 11));
    } else if (minute / 37 == 0) {
      second = cal.get(Calendar.SECOND);
      return (11 + randomGen.nextInt(60) - (minute % 5) - (second % 11));
    } else {
      second = cal.get(Calendar.SECOND);
      return (randomGen.nextInt(range) + (minute % 17) + (second % 11));
    }
  }

  private int genHdd(Calendar cal)
  {
    int minute = cal.get(Calendar.MINUTE);
    int second;
    int range = minute / 2 + 1;
    if (minute / 37 == 0) {
      second = cal.get(Calendar.SECOND);
      return (25 + randomGen.nextInt(range) - minute % 7 - second % 11);
    } else {
      second = cal.get(Calendar.SECOND);
      return (randomGen.nextInt(range) + minute % 23 + second % 11);
    }
  }

  /**
   * This method returns the minimum value for customer
   *
   * @return
   */
  public int getCustomerMin()
  {
    return customerMin;
  }

  /**
   * This method is used to set the minimum value for customer
   *
   * @param customerMin the minimum customer value
   */
  public void setCustomerMin(int customerMin)
  {
    this.customerMin = customerMin;
  }

  /**
   * This method returns the max value for customer
   *
   * @return
   */
  public int getCustomerMax()
  {
    return customerMax;
  }

  /**
   * This method is used to set the max value for customer
   *
   * @param customerMax the max customer value
   */
  public void setCustomerMax(int customerMax)
  {
    this.customerMax = customerMax;
  }

  /**
   * This method returns the minimum value for product
   *
   * @return
   */
  public int getProductMin()
  {
    return productMin;
  }

  /**
   * This method is used to set the minimum value for product
   *
   * @param productMin the minimum product value
   */
  public void setProductMin(int productMin)
  {
    this.productMin = productMin;
  }

  /**
   * This method returns the max value for product
   *
   * @return
   */
  public int getProductMax()
  {
    return productMax;
  }

  /**
   * This method is used to set the max value for product
   *
   * @param productMax the max product value
   */
  public void setProductMax(int productMax)
  {
    this.productMax = productMax;
  }

  /**
   * This method returns the minimum value for OS
   *
   * @return
   */
  public int getOsMin()
  {
    return osMin;
  }

  /**
   * This method is used to set the minimum value for OS
   *
   * @param osMin the min OS value
   */
  public void setOsMin(int osMin)
  {
    this.osMin = osMin;
  }

  /**
   * This method returns the max value for OS
   *
   * @return
   */
  public int getOsMax()
  {
    return osMax;
  }

  /**
   * This method is used to set the max value for OS
   *
   * @param osMax the max OS value
   */
  public void setOsMax(int osMax)
  {
    this.osMax = osMax;
  }

  /**
   * This method returns the minimum value for software1
   *
   * @return
   */
  public int getSoftware1Min()
  {
    return software1Min;
  }

  /**
   * This method is used to set the minimum value for software1
   *
   * @param software1Min the minimum software1 value
   */
  public void setSoftware1Min(int software1Min)
  {
    this.software1Min = software1Min;
  }

  /**
   * This method returns the max value for software1
   *
   * @return
   */
  public int getSoftware1Max()
  {
    return software1Max;
  }

  /**
   * This method is used to set the max value for software1
   *
   * @param software1Max the max software1 value
   */
  public void setSoftware1Max(int software1Max)
  {
    this.software1Max = software1Max;
  }

  /**
   * This method returns the minimum value for software2
   *
   * @return
   */
  public int getSoftware2Min()
  {
    return software2Min;
  }

  /**
   * This method is used to set the minimum value for software2
   *
   * @param software2Min the minimum software2 value
   */
  public void setSoftware2Min(int software2Min)
  {
    this.software2Min = software2Min;
  }

  /**
   * This method returns the max value for software2
   *
   * @return
   */
  public int getSoftware2Max()
  {
    return software2Max;
  }

  /**
   * This method is used to set the max value for software2
   *
   * @param software2Max the max software2 value
   */
  public void setSoftware2Max(int software2Max)
  {
    this.software2Max = software2Max;
  }

  /**
   * This method returns the minimum value for software3
   *
   * @return
   */
  public int getSoftware3Min()
  {
    return software3Min;
  }

  /**
   * This method is used to set the minimum value for software3
   *
   * @param software3Min the minimum software3 value
   */
  public void setSoftware3Min(int software3Min)
  {
    this.software3Min = software3Min;
  }

  /**
   * This method returns the max value for software3
   *
   * @return
   */
  public int getSoftware3Max()
  {
    return software3Max;
  }

  /**
   * This method is used to set the max value for software3
   *
   * @param software3Max the max software3 value
   */
  public void setSoftware3Max(int software3Max)
  {
    this.software3Max = software3Max;
  }

  /**
   * This method returns the minimum value for deviceId
   *
   * @return
   */
  public int getDeviceIdMin()
  {
    return deviceIdMin;
  }

  /**
   * This method is used to set the minimum value for deviceId
   *
   * @param deviceIdMin the minimum deviceId value
   */
  public void setDeviceIdMin(int deviceIdMin)
  {
    this.deviceIdMin = deviceIdMin;
  }

  /**
   * This method returns the max value for deviceId
   *
   * @return
   */
  public int getDeviceIdMax()
  {
    return deviceIdMax;
  }

  /**
   * This method is used to set the max value for deviceId
   *
   * @param deviceIdMax the max deviceId value
   */
  public void setDeviceIdMax(int deviceIdMax)
  {
    this.deviceIdMax = deviceIdMax;
  }

  /**
   * @return the tupleBlastSize
   */
  public int getTupleBlastSize()
  {
    return tupleBlastSize;
  }

  /**
   * @param tupleBlastSize the tupleBlastSize to set
   */
  public void setTupleBlastSize(int tupleBlastSize)
  {
    this.tupleBlastSize = tupleBlastSize;
  }
}
