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
package org.apache.apex.malhar.stream.sample.cookbook;

/**
 * Tuple class for JDBC input of {@link MaxPerKeyExamples}.
 *
 * @since 3.5.0
 */
public class InputPojo extends Object
{
  private int month;
  private int day;
  private int year;
  private double meanTemp;

  @Override
  public String toString()
  {
    return "PojoEvent [month=" + getMonth() + ", day=" + getDay() + ", year=" + getYear() + ", meanTemp=" + getMeanTemp() + "]";
  }

  public void setMonth(int month)
  {
    this.month = month;
  }

  public int getMonth()
  {
    return this.month;
  }

  public void setDay(int day)
  {
    this.day = day;
  }

  public int getDay()
  {
    return day;
  }

  public void setYear(int year)
  {
    this.year = year;
  }

  public int getYear()
  {
    return year;
  }

  public void setMeanTemp(double meanTemp)
  {
    this.meanTemp = meanTemp;
  }

  public double getMeanTemp()
  {
    return meanTemp;
  }
}
