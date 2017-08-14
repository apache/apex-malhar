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
package org.apache.apex.malhar.contrib.accumulo;

import java.io.Serializable;

public class AccumuloTuple implements Serializable
{
  private String row;
  private String colFamily;
  private String colName;
  private String colValue;

  public String getRow()
  {
    return row;
  }

  public void setRow(String row)
  {
    this.row = row;
  }

  public String getColFamily()
  {
    return colFamily;
  }

  public void setColFamily(String colFamily)
  {
    this.colFamily = colFamily;
  }

  public String getColName()
  {
    return colName;
  }

  public void setColName(String colName)
  {
    this.colName = colName;
  }

  public String getColValue()
  {
    return colValue;
  }

  public void setColValue(String colValue)
  {
    this.colValue = colValue;
  }

}
