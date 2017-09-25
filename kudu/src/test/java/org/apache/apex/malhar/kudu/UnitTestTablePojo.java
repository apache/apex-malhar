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


import java.nio.ByteBuffer;

public class UnitTestTablePojo
{
  private int introwkey;
  private String stringrowkey;
  private long timestamprowkey;
  private long longdata;
  private String stringdata;
  private long timestampdata;
  private ByteBuffer binarydata;
  private float floatdata;
  private boolean booldata;

  public int getIntrowkey()
  {
    return introwkey;
  }

  public void setIntrowkey(int introwkey)
  {
    this.introwkey = introwkey;
  }

  public String getStringrowkey()
  {
    return stringrowkey;
  }

  public void setStringrowkey(String stringrowkey)
  {
    this.stringrowkey = stringrowkey;
  }

  public long getTimestamprowkey()
  {
    return timestamprowkey;
  }

  public void setTimestamprowkey(long timestamprowkey)
  {
    this.timestamprowkey = timestamprowkey;
  }

  public long getLongdata()
  {
    return longdata;
  }

  public void setLongdata(long longdata)
  {
    this.longdata = longdata;
  }

  public String getStringdata()
  {
    return stringdata;
  }

  public void setStringdata(String stringdata)
  {
    this.stringdata = stringdata;
  }

  public long getTimestampdata()
  {
    return timestampdata;
  }

  public void setTimestampdata(long timestampdata)
  {
    this.timestampdata = timestampdata;
  }

  public ByteBuffer getBinarydata()
  {
    return binarydata;
  }

  public void setBinarydata(ByteBuffer binarydata)
  {
    this.binarydata = binarydata;
  }

  public float getFloatdata()
  {
    return floatdata;
  }

  public void setFloatdata(float floatdata)
  {
    this.floatdata = floatdata;
  }

  public boolean isBooldata()
  {
    return booldata;
  }

  public void setBooldata(boolean booldata)
  {
    this.booldata = booldata;
  }

  @Override
  public String toString()
  {
    return "UnitTestTablePojo{" +
      "introwkey=" + introwkey +
      ", stringrowkey='" + stringrowkey + '\'' +
      ", timestamprowkey=" + timestamprowkey +
      ", longdata=" + longdata +
      ", stringdata='" + stringdata + '\'' +
      ", timestampdata=" + timestampdata +
      ", binarydata=" + binarydata +
      ", floatdata=" + floatdata +
      ", booldata=" + booldata +
      '}';
  }
}
