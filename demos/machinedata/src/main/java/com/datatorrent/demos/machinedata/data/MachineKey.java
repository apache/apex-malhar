/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata.data;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;

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
  private String customer;
  private String product;
  private String os;
  private String software1;
  private String software2;
  private String deviceId;
  private String timeKey;
  private String day;
  private long timestamp;

  /**
   * This constructor takes the format in which time has to be captured and the day when this instance is created
   *
   * @param timestamp The time stamp associated with this key.
   * @param timeKey the format in which time has to be captured
   * @param day the day when this instance is created
   */
  public MachineKey(long timestamp, String timeKey, String day)
  {
    this.timestamp = timestamp;
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
   * @param deviceId deviceId
   */
  public MachineKey(String timeKey, String day, String customer, String product, String os, String software1, String software2, String deviceId)
  {
    this.timeKey = timeKey;
    this.day = day;
    this.customer = customer;
    this.product = product;
    this.os = os;
    this.software1 = software1;
    this.software2 = software2;
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
  public String getCustomer()
  {
    return customer;
  }

  /**
   * This method sets the customer Id
   *
   * @param customer
   *          the customer Id
   */
  public void setCustomer(String customer)
  {
    this.customer = customer;
  }

  /**
   * This method returns product on the device
   *
   * @return
   */
  public String getProduct()
  {
    return product;
  }

  /**
   * This method sets the product on the device
   *
   * @param product
   *          the value of product
   */
  public void setProduct(String product)
  {
    this.product = product;
  }

  /**
   * This method returns the OS version on the device
   *
   * @return
   */
  public String getOs()
  {
    return os;
  }

  /**
   * This method sets the OS version on the device
   *
   * @param os
   *          OS version
   */
  public void setOs(String os)
  {
    this.os = os;
  }

  /**
   * This method returns the version of the software1 on the device
   *
   * @return
   */
  public String getSoftware1()
  {
    return software1;
  }

  /**
   * This method sets the version of the software1 on the device
   *
   * @param software1 the version of the software1
   */
  public void setSoftware1(String software1)
  {
    this.software1 = software1;
  }

  /**
   * This method returns the version of the software2 on the device
   *
   * @return
   */
  public String getSoftware2()
  {
    return software2;
  }

  /**
   * This method sets the version of the software2 on the device
   *
   * @param software2
   *          the version of the software2
   */
  public void setSoftware2(String software2)
  {
    this.software2 = software2;
  }

  /**
   * This method returns the deviceId of the device
   * @return The deviceId
   */
  public String getDeviceId()
  {
    return deviceId;
  }

  /**
   * This method sets the deviceId of the device
   *
   * @param deviceId
   */
  public void setDeviceId(String deviceId)
  {
    this.deviceId = deviceId;
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

  @Override
  public int hashCode()
  {
    int hash = 7;
    hash = 67 * hash + Objects.hashCode(this.customer);
    hash = 67 * hash + Objects.hashCode(this.product);
    hash = 67 * hash + Objects.hashCode(this.os);
    hash = 67 * hash + Objects.hashCode(this.software1);
    hash = 67 * hash + Objects.hashCode(this.software2);
    hash = 67 * hash + Objects.hashCode(this.deviceId);
    hash = 67 * hash + (int) timestamp;
    hash = 67 * hash + Objects.hashCode(this.day);
    return hash;
  }

  public int hashCode(KeySelector ks)
  {
    int hash = 7;

    if (ks.useCustomer) {
      hash = hash * 67 + Objects.hashCode(this.customer);
    }

    if (ks.useProduct) {
      hash = hash * 67 + Objects.hashCode(this.product);
    }

    if (ks.useOs) {
      hash = hash * 67 + Objects.hashCode(this.os);
    }

    if (ks.useSoftware1) {
      hash = hash * 67 + Objects.hashCode(this.software1);
    }

    if (ks.useSoftware2) {
      hash = hash * 67 + Objects.hashCode(this.software2);
    }

    if (ks.useDeviceId) {
      hash = hash * 67 + Objects.hashCode(this.deviceId);
    }

    return hash;
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
    final MachineKey other = (MachineKey)obj;
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
    if (this.timestamp != other.timestamp) {
      return false;
    }
    if (!Objects.equals(this.day, other.day)) {
      return false;
    }
    return true;
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
    if (deviceId != null) {
      sb.append("|6:").append(deviceId);
    }
    return sb.toString();
  }

  public boolean equalsWithKey(MachineKey other, KeySelector ks)
  {
    if (ks.useCustomer && !Objects.equals(this.customer, other.customer)) {
      return false;
    }

    if (ks.useProduct && !Objects.equals(this.product, other.product)) {
      return false;
    }

    if (ks.useOs && !Objects.equals(this.os, other.os)) {
      return false;
    }

    if (ks.useSoftware1 && !Objects.equals(this.software1, other.software1)) {
      return false;
    }

    if (ks.useSoftware2 && !Objects.equals(this.software2, other.software2)) {
      return false;
    }

    if (ks.useDeviceId && !Objects.equals(this.deviceId, other.deviceId)) {
      return false;
    }

    return true;
  }

  public static class KeySelector implements Serializable
  {
    private static final long serialVersionUID = 201510251009L;

    public boolean useCustomer;
    public boolean useProduct;
    public boolean useOs;
    public boolean useSoftware1;
    public boolean useSoftware2;
    public boolean useDeviceId;

    public KeySelector()
    {
    }

    public KeySelector(Collection<String> fields)
    {
      useCustomer = fields.contains(MachineHardCodedAggregate.CUSTOMER_ID);
      useProduct = fields.contains(MachineHardCodedAggregate.PRODUCT_ID);
      useOs = fields.contains(MachineHardCodedAggregate.PRODUCT_OS);
      useSoftware1 = fields.contains(MachineHardCodedAggregate.SOFTWARE_1_VER);
      useSoftware2 = fields.contains(MachineHardCodedAggregate.SOFTWARE_2_VER);
      useDeviceId = fields.contains(MachineHardCodedAggregate.DEVICE_ID);
    }
  }
}
