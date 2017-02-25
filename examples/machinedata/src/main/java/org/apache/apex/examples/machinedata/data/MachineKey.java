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
package org.apache.apex.examples.machinedata.data;

/**
 * This class stores the information about various softwares, deviceIds, OS of the device
 * <p>
 * MachineKey class.
 * </p>
 *
 * @since 0.3.5
 */
public class MachineKey
{

  private Integer customer;
  private Integer product;
  private Integer os;
  private Integer software1;
  private Integer software2;
  private Integer software3;
  private Integer deviceId;
  private String timeKey;
  private String day;

  /**
   * This constructor takes the format in which time has to be captured and the day when this instance is created
   *
   * @param timeKey the format in which time has to be captured
   * @param day the day when this instance is created
   */
  public MachineKey(String timeKey, String day)
  {
    this.timeKey = timeKey;
    this.day = day;
  }

  /**
   * This is default constructor
   */
  public MachineKey()
  {
  }

  /**
   * This constructor takes format in which time has to be captured, the day when this instance is created, the customer
   * id, product Id on the device, OS version on the device, software1 version on the device, software2 version on the device,
   * software3 version on the device, deviceId on the device,
   *
   * @param timeKey the format in which time has to be captured
   * @param day the day when this instance is created
   * @param customer the customer Id
   * @param product product Id
   * @param os OS version
   * @param software1 software1 version
   * @param software2 software2 version
   * @param software3 software3 version
   * @param deviceId deviceId
   */
  public MachineKey(String timeKey, String day, Integer customer, Integer product, Integer os, Integer software1, Integer software2, Integer software3, Integer deviceId)
  {
    this.timeKey = timeKey;
    this.day = day;
    this.customer = customer;
    this.product = product;
    this.os = os;
    this.software1 = software1;
    this.software2 = software2;
    this.software3 = software3;
    this.deviceId = deviceId;
  }

  /**
   * This method returns the format in which the time is captured. The time is the time when this instance of MachineKey
   * was generated. For e.g. HHmm to capture Hour and minute
   *
   * @return
   */
  public String getTimeKey()
  {
    return timeKey;
  }

  /**
   * This method sets the format in which the time is captured. The time is the time when this instance of MachineKey
   * was generated. For e.g. HHmm to capture Hour and minute
   *
   * @param timeKey
   *          the value of format
   */
  public void setTimeKey(String timeKey)
  {
    this.timeKey = timeKey;
  }

  /**
   * This method returns the day of the month when this instance of MachineKey was generated
   *
   * @return
   */
  public String getDay()
  {
    return day;
  }

  /**
   * This method sets the day of the month when this instance of MachineKey was generated
   *
   * @param day
   *          the day of the month
   */
  public void setDay(String day)
  {
    this.day = day;
  }

  /**
   * This method returns the customer Id
   *
   * @return
   */
  public Integer getCustomer()
  {
    return customer;
  }

  /**
   * This method sets the customer Id
   *
   * @param customer
   *          the customer Id
   */
  public void setCustomer(Integer customer)
  {
    this.customer = customer;
  }

  /**
   * This method returns product on the device
   *
   * @return
   */
  public Integer getProduct()
  {
    return product;
  }

  /**
   * This method sets the product on the device
   *
   * @param product
   *          the value of product
   */
  public void setProduct(Integer product)
  {
    this.product = product;
  }

  /**
   * This method returns the OS version on the device
   *
   * @return
   */
  public Integer getOs()
  {
    return os;
  }

  /**
   * This method sets the OS version on the device
   *
   * @param os
   *          OS version
   */
  public void setOs(Integer os)
  {
    this.os = os;
  }

  /**
   * This method returns the version of the software1 on the device
   *
   * @return
   */
  public Integer getSoftware1()
  {
    return software1;
  }

  /**
   * This method sets the version of the software1 on the device
   *
   * @param software1 the version of the software1
   */
  public void setSoftware1(Integer software1)
  {
    this.software1 = software1;
  }

  /**
   * This method returns the version of the software2 on the device
   *
   * @return
   */
  public Integer getSoftware2()
  {
    return software2;
  }

  /**
   * This method sets the version of the software2 on the device
   *
   * @param software2
   *          the version of the software2
   */
  public void setSoftware2(Integer software2)
  {
    this.software2 = software2;
  }

  /**
   * This method returns the version of the software3 on the device
   *
   * @return
   */
  public Integer getSoftware3()
  {
    return software3;
  }

  /**
   * This method sets the version of the software3 on the device
   *
   * @param software3
   *          the version of the software3
   */
  public void setSoftware3(Integer software3)
  {
    this.software3 = software3;
  }

  @Override
  public int hashCode()
  {
    int key = 0;
    if (customer != null) {
      key |= (1 << 31);
      key ^= customer;
    }
    if (product != null) {
      key |= (1 << 30);
      key ^= product;
    }
    if (os != null) {
      key |= (1 << 29);
      key ^= os;
    }
    if (software1 != null) {
      key |= (1 << 28);
      key ^= software1;
    }
    if (software2 != null) {
      key |= (1 << 27);
      key ^= software2;
    }
    if (software3 != null) {
      key |= (1 << 26);
      key ^= software3;
    }
    if (deviceId != null) {
      key |= (1 << 25);
      key ^= deviceId;
    }
    if (timeKey != null) {
      key |= (1 << 24);
      key ^= timeKey.hashCode();
    }
    if (day != null) {
      key |= (1 << 23);
      key ^= day.hashCode();
    }

    return key;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (!(obj instanceof MachineKey)) {
      return false;
    }
    MachineKey mkey = (MachineKey)obj;
    return checkStringEqual(this.timeKey, mkey.timeKey) && checkStringEqual(this.day, mkey.day) && checkIntEqual(this.customer, mkey.customer) && checkIntEqual(this.product, mkey.product) && checkIntEqual(this.os, mkey.os) && checkIntEqual(this.software1, mkey.software1) && checkIntEqual(this.software2, mkey.software2) && checkIntEqual(this.software3, mkey.software3) && checkIntEqual(this.deviceId, mkey.deviceId);
  }

  private boolean checkIntEqual(Integer a, Integer b)
  {
    if ((a == null) && (b == null)) {
      return true;
    }
    if ((a != null) && a.equals(b)) {
      return true;
    }
    return false;
  }

  private boolean checkStringEqual(String a, String b)
  {
    if ((a == null) && (b == null)) {
      return true;
    }
    if ((a != null) && a.equals(b)) {
      return true;
    }
    return false;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(timeKey);
    if (customer != null) {
      sb.append("|0:").append(customer);
    }
    if (product != null) {
      sb.append("|1:").append(product);
    }
    if (os != null) {
      sb.append("|2:").append(os);
    }
    if (software1 != null) {
      sb.append("|3:").append(software1);
    }
    if (software2 != null) {
      sb.append("|4:").append(software2);
    }
    if (software3 != null) {
      sb.append("|5:").append(software3);
    }
    if (deviceId != null) {
      sb.append("|6:").append(deviceId);
    }
    return sb.toString();
  }

  /**
   * This method returns the deviceId of the device
   * @return The deviceId
   */
  public Integer getDeviceId()
  {
    return deviceId;
  }

  /**
   * This method sets the deviceId of the device
   *
   * @param deviceId
   */
  public void setDeviceId(Integer deviceId)
  {
    this.deviceId = deviceId;
  }

}
