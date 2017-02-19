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

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import static org.apache.apex.malhar.flume.interceptor.ColumnFilteringInterceptor.Constants.COLUMNS;
import static org.apache.apex.malhar.flume.interceptor.ColumnFilteringInterceptor.Constants.DST_SEPARATOR;
import static org.apache.apex.malhar.flume.interceptor.ColumnFilteringInterceptor.Constants.DST_SEPARATOR_DFLT;
import static org.apache.apex.malhar.flume.interceptor.ColumnFilteringInterceptor.Constants.SRC_SEPARATOR;
import static org.apache.apex.malhar.flume.interceptor.ColumnFilteringInterceptor.Constants.SRC_SEPARATOR_DFLT;

/**
 * <p>ColumnFilteringInterceptor class.</p>
 *
 * @since 0.9.4
 */
public class ColumnFilteringInterceptor implements Interceptor
{
  private final byte srcSeparator;
  private final byte dstSeparator;

  private final int maxIndex;
  private final int maxColumn;
  private final int[] columns;
  private final int[] positions;

  private ColumnFilteringInterceptor(int[] columns, byte srcSeparator, byte dstSeparator)
  {
    this.columns = columns;

    int tempMaxColumn = Integer.MIN_VALUE;
    for (int column: columns) {
      if (column > tempMaxColumn) {
        tempMaxColumn = column;
      }
    }
    maxIndex = tempMaxColumn;
    maxColumn = tempMaxColumn + 1;
    positions = new int[maxColumn + 1];

    this.srcSeparator = srcSeparator;
    this.dstSeparator = dstSeparator;
  }

  @Override
  public void initialize()
  {
    /* no-op */
  }

  @Override
  public Event intercept(Event event)
  {
    byte[] body = event.getBody();
    if (body == null) {
      return event;
    }

    final int length = body.length;

    /* store positions of character after the separators */
    int i = 0;
    int index = 0;
    while (i < length) {
      if (body[i++] == srcSeparator) {
        positions[++index] = i;
        if (index >= maxIndex) {
          break;
        }
      }
    }

    int nextVirginIndex;
    boolean separatorTerminated;
    if (i == length && index < maxColumn) {
      nextVirginIndex = index + 2;
      positions[nextVirginIndex - 1] = length;
      separatorTerminated = length > 0 ? body[length - 1]  != srcSeparator : false;
    } else {
      nextVirginIndex = index + 1;
      separatorTerminated = true;
    }

    int newArrayLen = 0;
    for (i = columns.length; i-- > 0;) {
      int column = columns[i];
      int len = positions[column + 1] - positions[column];
      if (len <= 0) {
        newArrayLen++;
      } else {
        if (separatorTerminated && positions[column + 1] == length) {
          newArrayLen++;
        }
        newArrayLen += len;
      }
    }

    byte[] newbody = new byte[newArrayLen];
    int newoffset = 0;
    for (int column: columns) {
      int len = positions[column + 1] - positions[column];
      if (len > 0) {
        System.arraycopy(body, positions[column], newbody, newoffset, len);
        newoffset += len;
        if (newbody[newoffset - 1] == srcSeparator) {
          newbody[newoffset - 1] = dstSeparator;
        } else {
          newbody[newoffset++] = dstSeparator;
        }
      } else {
        newbody[newoffset++] = dstSeparator;
      }
    }

    event.setBody(newbody);
    Arrays.fill(positions, 1, nextVirginIndex, 0);
    return event;
  }

  @Override
  public List<Event> intercept(List<Event> events)
  {
    for (Event event: events) {
      intercept(event);
    }
    return events;
  }

  @Override
  public void close()
  {
  }

  public static class Builder implements Interceptor.Builder
  {
    private int[] columns;
    private byte srcSeparator;
    private byte dstSeparator;

    @Override
    public Interceptor build()
    {
      return new ColumnFilteringInterceptor(columns, srcSeparator, dstSeparator);
    }

    @Override
    public void configure(Context context)
    {
      String sColumns = context.getString(COLUMNS);
      if (sColumns == null || sColumns.trim().isEmpty()) {
        throw new Error("This interceptor requires filtered columns to be specified!");
      }

      String[] parts = sColumns.split(" ");
      columns = new int[parts.length];
      for (int i = parts.length; i-- > 0;) {
        columns[i] = Integer.parseInt(parts[i]);
      }

      srcSeparator = context.getInteger(SRC_SEPARATOR, (int)SRC_SEPARATOR_DFLT).byteValue();
      dstSeparator = context.getInteger(DST_SEPARATOR, (int)DST_SEPARATOR_DFLT).byteValue();
    }

  }

  @SuppressWarnings("ClassMayBeInterface") /* adhering to flume until i understand it completely */

  public static class Constants
  {
    public static final String SRC_SEPARATOR = "srcSeparator";
    public static final byte SRC_SEPARATOR_DFLT = 2;

    public static final String DST_SEPARATOR = "dstSeparator";
    public static final byte DST_SEPARATOR_DFLT = 1;

    public static final String COLUMNS = "columns";
  }

  private static final Logger logger = LoggerFactory.getLogger(ColumnFilteringInterceptor.class);
}
