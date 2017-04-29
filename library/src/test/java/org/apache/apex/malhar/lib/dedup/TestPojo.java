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
package org.apache.apex.malhar.lib.dedup;

import java.util.Date;

public class TestPojo
{
  private long key;
  private Date date;
  public long sequence;

  public TestPojo()
  {
  }

  public TestPojo(long key, Date date)
  {
    this.key = key;
    this.date = date;
  }

  public TestPojo(long key, Date date, long sequence)
  {
    this.key = key;
    this.date = date;
    this.sequence = sequence;
  }

  public long getKey()
  {
    return key;
  }

  public Date getDate()
  {
    return date;
  }

  public void setKey(long key)
  {
    this.key = key;
  }

  public void setDate(Date date)
  {
    this.date = date;
  }

  public long getSequence()
  {
    return sequence;
  }

  public void setSequence(long sequence)
  {
    this.sequence = sequence;
  }

  @Override
  public String toString()
  {
    return "TestPojo [key=" + key + ", date=" + date + ", sequence=" + sequence + "]";
  }

}
