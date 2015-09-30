/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata.data;

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
  private int cpu;
  private int ram;
  private int hdd;

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
  public MachineInfo(MachineKey machineKey, int cpu, int ram, int hdd)
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
  public int getCpu()
  {
    return cpu;
  }

  /**
   * This method sets the CPU% usage
   *
   * @param cpu the CPU% usage
   */
  public void setCpu(int cpu)
  {
    this.cpu = cpu;
  }

  /**
   * This method returns the RAM% usage
   *
   * @return
   */
  public int getRam()
  {
    return ram;
  }

  /**
   * This method sets the RAM% usage
   *
   * @param ram the RAM% usage
   */
  public void setRam(int ram)
  {
    this.ram = ram;
  }

  /**
   * This method returns the HDD% usage
   *
   * @return
   */
  public int getHdd()
  {
    return hdd;
  }

  /**
   * This method sets the HDD% usage
   *
   * @param hdd the HDD% usage
   */
  public void setHdd(int hdd)
  {
    this.hdd = hdd;
  }

}
