/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.accumulo;

public class AccumuloTuple {
  private String row;
  private String columnFamily;
  private String columnQualifier;
  private long timestamp;
  private String columnVisibility;
  private String columnValue;

  public String getColumnValue()
  {
    return columnValue;
  }


  public String getColumnVisibility()
  {
    return columnVisibility;
  }

  public void setColumnVisibility(String columnVisibility)
  {
    this.columnVisibility = columnVisibility;
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

  public String getColumnFamily()
  {
    return columnFamily;
  }

  public void setColumnFamily(String columnFamily)
  {
    this.columnFamily = columnFamily;
  }

  public String getColumnQualifier()
  {
    return columnQualifier;
  }

  public void setColumnQualifier(String columnQualifier)
  {
    this.columnQualifier = columnQualifier;
  }

  public String getRow() {
    return row;
  }

  public void setRow(String row) {
    this.row = row;
  }

  public void setColumnValue(String columnValue)
  {
    this.columnValue = columnValue;
  }


}
