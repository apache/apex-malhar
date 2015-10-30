/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata.data;

import com.datatorrent.demos.machinedata.data.MachineKey.KeySelector;

/**
 * This class stores the cpu% usage, ram% usage, hdd% usage and key information about a particular machine
 * <p>
 * MachineInfo class.
 * </p>
 *
 * @since 0.3.5
 */
public class MachineInfo
{
  private MachineKey machineKey;
  private long cpu;
  private long ram;
  private long hdd;

  /**
   * This default constructor
   */
  public MachineInfo()
  {
  }

  /**
   * This constructor takes MachineKey as input and initialize local attributes
   *
   * @param machineKey the MachineKey instance
   */
  public MachineInfo(MachineKey machineKey)
  {
    this.machineKey = machineKey;
  }

  /**
   * This constructor takes MachineKey, cpu usage, ram usage, hdd usage as input and initialize local attributes
   *
   * @param machineKey the MachineKey instance
   * @param cpu the CPU% usage
   * @param ram the RAM% usage
   * @param hdd the HDD% usage
   */
  public MachineInfo(MachineKey machineKey, long cpu, long ram, long hdd)
  {
    this.machineKey = machineKey;
    this.cpu = cpu;
    this.ram = ram;
    this.hdd = hdd;
  }

  /**
   * This method returns the MachineKey
   *
   * @return
   */
  public MachineKey getMachineKey()
  {
    return machineKey;
  }

  /**
   * This method sets the MachineKey
   *
   * @param machineKey the MachineKey instance
   */
  public void setMachineKey(MachineKey machineKey)
  {
    this.machineKey = machineKey;
  }

  /**
   * This method returns the CPU% usage
   *
   * @return
   */
  public long getCpu()
  {
    return cpu;
  }

  /**
   * This method sets the CPU% usage
   *
   * @param cpu the CPU% usage
   */
  public void setCpu(long cpu)
  {
    this.cpu = cpu;
  }

  /**
   * This method returns the RAM% usage
   *
   * @return
   */
  public long getRam()
  {
    return ram;
  }

  /**
   * This method sets the RAM% usage
   *
   * @param ram the RAM% usage
   */
  public void setRam(long ram)
  {
    this.ram = ram;
  }

  /**
   * This method returns the HDD% usage
   *
   * @return
   */
  public long getHdd()
  {
    return hdd;
  }

  /**
   * This method sets the HDD% usage
   *
   * @param hdd the HDD% usage
   */
  public void setHdd(long hdd)
  {
    this.hdd = hdd;
  }

  public boolean equalsWithKey(MachineInfo other, KeySelector ks)
  {
    return this.getMachineKey().equalsWithKey(other.getMachineKey(), ks);
  }
}
