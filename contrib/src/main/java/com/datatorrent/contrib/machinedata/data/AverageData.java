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
 * This class stores the value of sum and the count of values summed.
 * <p>
 * AverageData class.
 * </p>
 * 
 * @since 0.3.5
 */
public class AverageData
{

  private double sum;
  private long count;

  /**
   * This is default constructor that sets the sum and count to 0
   */
  public AverageData()
  {
    sum = 0;
    count = 0;
  }

  /**
   * This constructor takes the value of sum and count and initialize the local attributes to corresponding values
   * 
   * @param sum
   *          the value of sum
   * @param count
   *          the value of count
   */
  public AverageData(double sum, long count)
  {
    this.sum = sum;
    this.count = count;
  }

  /**
   * This returns the value of sum
   * @return
   */
  public double getSum()
  {
    return sum;
  }

  /**
   * This method sets the value of sum
   * @param sum
   */
  public void setSum(double sum)
  {
    this.sum = sum;
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
