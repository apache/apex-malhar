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
package org.apache.apex.malhar.contrib.splunk;

import javax.validation.constraints.NotNull;

/**
 * @since 2.1.0
 */
public class SplunkInputOperator extends AbstractSplunkInputOperator<String>
{
  @NotNull
  private String query = "search * | head 100";

  @Override
  public String getTuple(String value)
  {
    return value;
  }

  @Override
  public String queryToRetrieveData()
  {
    return query;
  }

  /*
   * Query to retrieve data from Splunk.
   */
  public String getQuery()
  {
    return query;
  }

  public void setQuery(String query)
  {
    this.query = query;
  }
}
