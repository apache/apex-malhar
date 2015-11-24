/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata.data;

import java.util.Objects;

import com.datatorrent.lib.statistics.DimensionsComputation.AggregateEvent;

/**
 * @since 3.2.0
 */
public class MachineHardCodedAggregate implements AggregateEvent
{
  public static final String CUSTOMER_ID = "Customer ID";
  public static final String PRODUCT_ID = "Product ID";
  public static final String PRODUCT_OS = "Product OS";
  public static final String SOFTWARE_1_VER = "Software1 Ver";
  public static final String SOFTWARE_2_VER = "Software2 Ver";
  public static final String DEVICE_ID = "Device ID";

  public long timestamp;

  public String customer;
  public String product;
  public String os;
  public String software1;
  public String software2;
  public String deviceId;

  public long cpuUsage;
  public long ramUsage;
  public long hddUsage;

  public long count;
  public boolean sum;

  public int ddID;
  public int aggregatorIndex;

  public MachineHardCodedAggregate()
  {
  }

  /**
   * @return the timestamp
   */
  public long getTimestamp()
  {
    return timestamp;
  }

  /**
   * @param timestamp the timestamp to set
   */
  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

  /**
   * @return the customer
   */
  public String getCustomer()
  {
    return customer;
  }

  /**
   * @param customer the customer to set
   */
  public void setCustomer(String customer)
  {
    this.customer = customer;
  }

  /**
   * @return the product
   */
  public String getProduct()
  {
    return product;
  }

  /**
   * @param product the product to set
   */
  public void setProduct(String product)
  {
    this.product = product;
  }

  /**
   * @return the os
   */
  public String getOs()
  {
    return os;
  }

  /**
   * @param os the os to set
   */
  public void setOs(String os)
  {
    this.os = os;
  }

  /**
   * @return the software1
   */
  public String getSoftware1()
  {
    return software1;
  }

  /**
   * @param software1 the software1 to set
   */
  public void setSoftware1(String software1)
  {
    this.software1 = software1;
  }

  /**
   * @return the software2
   */
  public String getSoftware2()
  {
    return software2;
  }

  /**
   * @param software2 the software2 to set
   */
  public void setSoftware2(String software2)
  {
    this.software2 = software2;
  }

  /**
   * @return the deviceId
   */
  public String getDeviceId()
  {
    return deviceId;
  }

  /**
   * @param deviceId the deviceId to set
   */
  public void setDeviceId(String deviceId)
  {
    this.deviceId = deviceId;
  }

  /**
   * @return the cpuUsage
   */
  public long getCpuUsage()
  {
    return cpuUsage;
  }

  /**
   * @param cpuUsage the cpuUsage to set
   */
  public void setCpuUsage(long cpuUsage)
  {
    this.cpuUsage = cpuUsage;
  }

  /**
   * @return the ramUsage
   */
  public long getRamUsage()
  {
    return ramUsage;
  }

  /**
   * @param ramUsage the ramUsage to set
   */
  public void setRamUsage(long ramUsage)
  {
    this.ramUsage = ramUsage;
  }

  /**
   * @return the hddUsage
   */
  public long getHddUsage()
  {
    return hddUsage;
  }

  /**
   * @param hddUsage the hddUsage to set
   */
  public void setHddUsage(long hddUsage)
  {
    this.hddUsage = hddUsage;
  }

  /**
   * @return the count
   */
  public long getCount()
  {
    return count;
  }

  /**
   * @param count the count to set
   */
  public void setCount(long count)
  {
    this.count = count;
  }

  /**
   * @return the sum
   */
  public boolean isSum()
  {
    return sum;
  }

  /**
   * @param sum the sum to set
   */
  public void setSum(boolean sum)
  {
    this.sum = sum;
  }

  /**
   * @return the ddID
   */
  public int getDdID()
  {
    return ddID;
  }

  /**
   * @param ddID the ddID to set
   */
  public void setDdID(int ddID)
  {
    this.ddID = ddID;
  }

  public void setAggregatorIndex(int aggregatorIndex)
  {
    this.aggregatorIndex = aggregatorIndex;
  }

  @Override
  public int getAggregatorIndex()
  {
    return aggregatorIndex;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(this.timestamp,
                        this.customer,
                        this.product,
                        this.os,
                        this.software1,
                        this.software2,
                        this.deviceId,
                        this.ddID,
                        this.sum);
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final MachineHardCodedAggregate other = (MachineHardCodedAggregate)obj;
    if (this.timestamp != other.timestamp) {
      return false;
    }
    if (!Objects.equals(this.customer, other.customer)) {
      return false;
    }
    if (!Objects.equals(this.product, other.product)) {
      return false;
    }
    if (!Objects.equals(this.os, other.os)) {
      return false;
    }
    if (!Objects.equals(this.software1, other.software1)) {
      return false;
    }
    if (!Objects.equals(this.software2, other.software2)) {
      return false;
    }
    if (!Objects.equals(this.deviceId, other.deviceId)) {
      return false;
    }
    if (this.ddID != other.ddID) {
      return false;
    }
    return this.sum == other.sum;
  }

}
