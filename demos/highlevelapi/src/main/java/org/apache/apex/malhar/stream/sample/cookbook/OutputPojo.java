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
 * OutputPojo Tuple Class for jdbcOutput of {@link MaxPerKeyExamples}.
 *
 * @since 3.5.0
 */
public class OutputPojo
{
  private int month;
  private double meanTemp;

  @Override
  public String toString()
  {
    return "PojoEvent [month=" + getMonth() + ", meanTemp=" + getMeanTemp() + "]";
  }

  public void setMonth(int month)
  {
    this.month = month;
  }

  public int getMonth()
  {
    return this.month;
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
