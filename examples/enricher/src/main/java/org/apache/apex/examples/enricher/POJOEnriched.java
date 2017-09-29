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

package org.apache.apex.examples.enricher;

/**
 * @since 3.7.0
 */
public class POJOEnriched
{
  private String phone;
  private String imei;
  private String imsi;
  private int circleId;
  private String circleName;

  public String getPhone()
  {
    return phone;
  }

  public void setPhone(String phone)
  {
    this.phone = phone;
  }

  public String getImei()
  {
    return imei;
  }

  public void setImei(String imei)
  {
    this.imei = imei;
  }

  public String getImsi()
  {
    return imsi;
  }

  public void setImsi(String imsi)
  {
    this.imsi = imsi;
  }

  public int getCircleId()
  {
    return circleId;
  }

  public void setCircleId(int circleId)
  {
    this.circleId = circleId;
  }

  public String getCircleName()
  {
    return circleName;
  }

  public void setCircleName(String circleName)
  {
    this.circleName = circleName;
  }

  @Override
  public String toString()
  {
    return "{" +
        "\"phone\":\"" + phone + "\"," +
        "\"imei\":\"" + imei + "\"," +
        "\"imsi\":\"" + imsi + "\"," +
        "\"circleId\":" + circleId + "," +
        "\"circleName\":\"" + circleName + "\"" +
        "}";
  }
}
