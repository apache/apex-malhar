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
 * This class stores the value of sum and the count of values summed.
 * <p>
 * AverageData class.
 * </p>
 *
 * @since 0.3.5
 */
public class AverageData
{

  private long cpu;
  private long hdd;
  private long ram;
  private long count;

  /**
   * This is default constructor that sets the sum and count to 0
   */
  public AverageData()
  {

  }

  /**
   * This constructor takes the value of sum and count and initialize the local attributes to corresponding values
   *
   * @param count
   *          the value of count
   */
  public AverageData(long cpu,long hdd,long ram, long count)
  {
    this.cpu = cpu;
    this.ram = ram;
    this.hdd = hdd;
    this.count = count;
  }

  public long getCpu()
  {
    return cpu;
  }

  public void setCpu(long cpu)
  {
    this.cpu = cpu;
  }

  public long getHdd()
  {
    return hdd;
  }

  public void setHdd(long hdd)
  {
    this.hdd = hdd;
  }

  public long getRam()
  {
    return ram;
  }

  public void setRam(long ram)
  {
    this.ram = ram;
  }

  /**
   * This returns the value of count
   * @return
   */
  public long getCount()
  {
    return count;
  }

  /**
   * This method sets the value of count
   * @param count
   */
  public void setCount(long count)
  {
    this.count = count;
  }
}
