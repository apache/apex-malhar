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
package com.datatorrent.contrib.machinedata.data;


/**
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
  private Integer day; 

  public MachineKey(String timeKey, Integer day)
  {
    this.timeKey = timeKey;
    this.day = day;
  }

  public MachineKey()
  { 
  }

  public MachineKey(String timeKey, Integer day,Integer customer, Integer product, Integer os, Integer software1, Integer software2, Integer software3, Integer deviceId)
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

  public Integer getCustomer()
  {
    return customer;
  }

  public String getTimeKey()
  {
    return timeKey;
  }

  public void setTimeKey(String timeKey)
  {
    this.timeKey = timeKey;
  }

  public Integer getDay()
  {
    return day;
  }

  public void setDay(Integer day)
  {
    this.day = day;
  }

  public void setCustomer(Integer customer)
  {
    this.customer = customer;
  }

  public Integer getProduct()
  {
    return product;
  }

  public void setProduct(Integer product)
  {
    this.product = product;
  }

  public Integer getOs()
  {
    return os;
  }

  public void setOs(Integer os)
  {
    this.os = os;
  }

  public Integer getSoftware1()
  {
    return software1;
  }

  public void setSoftware1(Integer software1)
  {
    this.software1 = software1;
  }

  public Integer getSoftware2()
  {
    return software2;
  }

  public void setSoftware2(Integer software2)
  {
    this.software2 = software2;
  }

  public Integer getSoftware3()
  {
    return software3;
  }

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
      key ^= customer ;
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
      key ^= day;
    }
    
    return key;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (!(obj instanceof MachineKey)) {
      return false;
    }
    MachineKey mkey = (MachineKey) obj;
    return checkStringEqual(this.timeKey, mkey.timeKey) && checkIntEqual(this.day, mkey.day) && checkIntEqual(this.customer, mkey.customer) && checkIntEqual(this.product, mkey.product) && checkIntEqual(this.os, mkey.os) && checkIntEqual(this.software1, mkey.software1) && checkIntEqual(this.software2, mkey.software2) && checkIntEqual(this.software3, mkey.software3) && checkIntEqual(this.deviceId, mkey.deviceId);
  }

  private boolean checkIntEqual(Integer a, Integer b)
  {
    if ((a == null) && (b == null))
      return true;
    if ((a != null) && a.equals(b))
      return true;
    return false;
  }
  private boolean checkStringEqual(String a, String b)
  {
    if ((a == null) && (b == null))
      return true;
    if ((a != null) && a.equals(b))
      return true;
    return false;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(timeKey);
    if (customer != null)
      sb.append("|0:").append(customer);
    if (product != null)
      sb.append("|1:").append(product);
    if (os != null)
      sb.append("|2:").append(os);
    if (software1 != null)
      sb.append("|3:").append(software1);
    if (software2 != null)
      sb.append("|4:").append(software2);
    if (software3 != null)
      sb.append("|5:").append(software3);
    if (deviceId != null)
      sb.append("|6:").append(deviceId);
    return sb.toString();
  }

  public Integer getDeviceId()
  {
    return deviceId;
  }

  public void setDeviceId(Integer deviceId)
  {
    this.deviceId = deviceId;
  }

}
