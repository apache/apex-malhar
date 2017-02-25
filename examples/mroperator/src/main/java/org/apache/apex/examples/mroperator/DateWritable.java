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
package org.apache.apex.examples.mroperator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

/**
 * <p>DateWritable class.</p>
 *
 * @since 0.9.0
 */
public class DateWritable implements WritableComparable<DateWritable>
{
  private static final SimpleDateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd' T 'HH:mm:ss.SSS" );
  private Date date;

  public Date getDate()
  {
    return date;
  }

  public void setDate( Date date )
  {
    this.date = date;
  }

  public void readFields( DataInput in ) throws IOException
  {
    date = new Date( in.readLong() );
  }

  public void write( DataOutput out ) throws IOException
  {
    out.writeLong( date.getTime() );
  }

  @Override
  public boolean equals(Object o)
  {
    return toString().equals(o.toString());
  }

  @Override
  public int hashCode()
  {
    return toString().hashCode();
  }

  public String toString()
  {
    return formatter.format( date);
  }

  public int compareTo( DateWritable other )
  {
    return date.compareTo( other.getDate() );
  }
}
