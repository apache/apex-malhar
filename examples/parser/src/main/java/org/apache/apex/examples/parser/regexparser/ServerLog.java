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
package org.apache.apex.examples.parser.regexparser;

import java.util.Date;

/**
 * @since 3.8.0
 */
public class ServerLog
{
  private Date date;
  private int id;
  private String signInId;
  private String ipAddress;
  private String serviceId;
  private String accountId;
  private String platform;

  public int getId()
  {
    return id;
  }

  public void setId(int id)
  {
    this.id = id;
  }

  public Date getDate()
  {
    return date;
  }

  public void setDate(Date date)
  {
    this.date = date;
  }

  public String getSignInId()
  {
    return signInId;
  }

  public void setSignInId(String signInId)
  {
    this.signInId = signInId;
  }

  public String getIpAddress()
  {
    return ipAddress;
  }

  public void setIpAddress(String ipAddress)
  {
    this.ipAddress = ipAddress;
  }

  public String getServiceId()
  {
    return serviceId;
  }

  public void setServiceId(String serviceId)
  {
    this.serviceId = serviceId;
  }

  public String getAccountId()
  {
    return accountId;
  }

  public void setAccountId(String accountId)
  {
    this.accountId = accountId;
  }

  public String getPlatform()
  {
    return platform;
  }

  public void setPlatform(String platform)
  {
    this.platform = platform;
  }

  @Override
  public String toString()
  {
    return "ServerLog{" +
      "date=" + date +
      ", id=" + id +
      ", signInId='" + signInId + '\'' +
      ", ipAddress='" + ipAddress + '\'' +
      ", serviceId='" + serviceId + '\'' +
      ", accountId='" + accountId + '\'' +
      ", platform='" + platform + '\'' +
      '}';
  }
}
