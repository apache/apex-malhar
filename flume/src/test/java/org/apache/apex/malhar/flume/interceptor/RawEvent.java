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
package org.apache.apex.malhar.flume.interceptor;

import java.io.Serializable;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.util.Slice;

/**
 *
 */
public class RawEvent implements Serializable
{
  public Slice guid;
  public long time;
  public int dimensionsOffset;

  public Slice getGUID()
  {
    return guid;
  }

  public long getTime()
  {
    return time;
  }

  RawEvent()
  {
    /* needed for Kryo serialization */
  }

  public static RawEvent from(byte[] row, byte separator)
  {
    final int rowsize = row.length;

    /*
     * Lets get the guid out of the current record
     */
    int sliceLengh = -1;
    while (++sliceLengh < rowsize) {
      if (row[sliceLengh] == separator) {
        break;
      }
    }

    int i = sliceLengh + 1;

    /* lets parse the date */
    int dateStart = i;
    while (i < rowsize) {
      if (row[i++] == separator) {
        long time = DATE_PARSER.parseMillis(new String(row, dateStart, i - dateStart - 1));
        RawEvent event = new RawEvent();
        event.guid = new Slice(row, 0, sliceLengh);
        event.time = time;
        event.dimensionsOffset = i;
        return event;
      }
    }

    return null;
  }

  @Override
  public int hashCode()
  {
    int hash = 5;
    hash = 61 * hash + (this.guid != null ? this.guid.hashCode() : 0);
    hash = 61 * hash + (int)(this.time ^ (this.time >>> 32));
    return hash;
  }

  @Override
  public String toString()
  {
    return "RawEvent{" + "guid=" + guid + ", time=" + time + '}';
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final RawEvent other = (RawEvent)obj;
    if (this.guid != other.guid && (this.guid == null || !this.guid.equals(other.guid))) {
      return false;
    }
    return this.time == other.time;
  }

  private static final DateTimeFormatter DATE_PARSER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
  private static final Logger logger = LoggerFactory.getLogger(RawEvent.class);
  private static final long serialVersionUID = 201312191312L;
}
