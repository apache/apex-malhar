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
package org.apache.apex.malhar.contrib.parser.log;

import org.apache.apex.malhar.contrib.parser.LogParser;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is default log format parser for <a href="https://en.wikipedia.org/wiki/Common_Log_Format">Common log </a>
 * To use this format with {@link LogParser} operator just mention the property "logFileFormat" as "COMMON"
 *
 * @since 3.7.0
 */
@InterfaceStability.Evolving
public class CommonLog
{
  private String host;
  private String rfc931;
  private String username;
  private String datetime;
  private String request;
  private String statusCode;
  private String bytes;

  @Override
  public String toString()
  {
    return "CommonLog [ Host : " + this.getHost() +
      ", rfc931 : " + this.getRfc931() +
      ", userName : " + this.getUsername() +
      ", dateTime : " + this.getDatetime() +
      ", request : " + this.getRequest() +
      ", statusCode : " + this.getStatusCode() +
      ", bytes : " + this.getBytes() + " ]";
  }

  public String getHost()
  {
    return host;
  }

  public void setHost(String host)
  {
    this.host = host;
  }

  public String getRfc931()
  {
    return rfc931;
  }

  public void setRfc931(String rfc931)
  {
    this.rfc931 = rfc931;
  }

  public String getUsername()
  {
    return username;
  }

  public void setUsername(String username)
  {
    this.username = username;
  }

  public String getDatetime()
  {
    return datetime;
  }

  public void setDatetime(String datetime)
  {
    this.datetime = datetime;
  }

  public String getRequest()
  {
    return request;
  }

  public void setRequest(String request)
  {
    this.request = request;
  }

  public String getStatusCode()
  {
    return statusCode;
  }

  public void setStatusCode(String statusCode)
  {
    this.statusCode = statusCode;
  }

  public String getBytes()
  {
    return bytes;
  }

  public void setBytes(String bytes)
  {
    this.bytes = bytes;
  }
}
