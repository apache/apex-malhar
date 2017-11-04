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
package org.apache.apex.malhar.kudu;

import org.apache.apex.api.operator.ControlTuple;

/**
 * A simple control tuple class that is used to represent a begin or end of a given SQL expression.
 *
 * @since 3.8.0
 */
public class InputOperatorControlTuple implements ControlTuple
{
  private String query;

  private boolean beginNewQuery;

  private boolean endCurrentQuery;

  private String controlMessage;

  @Override
  public DeliveryType getDeliveryType()
  {
    return DeliveryType.IMMEDIATE;
  }

  public String getQuery()
  {
    return query;
  }

  public void setQuery(String query)
  {
    this.query = query;
  }

  public boolean isBeginNewQuery()
  {
    return beginNewQuery;
  }

  public void setBeginNewQuery(boolean beginNewQuery)
  {
    this.beginNewQuery = beginNewQuery;
  }

  public boolean isEndCurrentQuery()
  {
    return endCurrentQuery;
  }

  public void setEndCurrentQuery(boolean endCurrentQuery)
  {
    this.endCurrentQuery = endCurrentQuery;
  }

  public String getControlMessage()
  {
    return controlMessage;
  }

  public void setControlMessage(String controlMessage)
  {
    this.controlMessage = controlMessage;
  }
}
