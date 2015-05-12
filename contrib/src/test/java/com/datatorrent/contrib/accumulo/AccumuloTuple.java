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

//import java.io.Serializable;

public class AccumuloTuple {
  private String row;
  private String columnFamily;
  private String columnName;
  private String columnValue;
  private String columnQualifier;
  private long timestamp;
  private String columnVisibility;
  private int userid;

  public int getUserid()
  {
    return userid;
  }

  public void setUserid(int userid)
  {
    this.userid = userid;
  }

  public int getAge()
  {
    return age;
  }

  public void setAge(int age)
  {
    this.age = age;
  }

  public String getAddress()
  {
    return address;
  }

  public void setAddress(String address)
  {
    this.address = address;
  }

  public String getAccount_balance()
  {
    return account_balance;
  }

  public void setAccount_balance(String account_balance)
  {
    this.account_balance = account_balance;
  }
  private int age;
  private String address;
  private String account_balance;

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

  public String getColumnName()
  {
    return columnName;
  }

  public void setColumnName(String columnName)
  {
    this.columnName = columnName;
  }

  public String getColumnValue()
  {
    return columnValue;
  }

  public void setColumnValue(String columnValue)
  {
    this.columnValue = columnValue;
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


}