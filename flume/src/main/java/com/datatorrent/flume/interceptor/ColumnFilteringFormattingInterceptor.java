/*
 * Copyright (c) 2013 DataTorrent, Inc.
 * All Rights Reserved.
 */
package com.datatorrent.flume.interceptor;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import static com.datatorrent.flume.interceptor.ColumnFilteringFormattingInterceptor.Constants.COLUMNS_FORMATTER;
import static com.datatorrent.flume.interceptor.ColumnFilteringFormattingInterceptor.Constants.SRC_SEPARATOR;
import static com.datatorrent.flume.interceptor.ColumnFilteringFormattingInterceptor.Constants.SRC_SEPARATOR_DFLT;

/**
 * <p>ColumnFilteringFormattingInterceptor class.</p>
 *
 * @author Chandni Singh <chandni@datatorrent.com>
 * @since 0.9.4
 */
public class ColumnFilteringFormattingInterceptor implements Interceptor
{
  private final byte srcSeparator;
  private final byte[][] dstSeparators;
  private final byte[] prefix;
  private final int maxIndex;
  private final int maxColumn;
  private final int[] columns;
  private final int[] positions;

  private ColumnFilteringFormattingInterceptor(int[] columns, byte srcSeparator, byte[][] dstSeparators, byte[] prefix)
  {
    this.columns = columns;

    int tempMaxColumn = Integer.MIN_VALUE;
    for (int column : columns) {
      if (column > tempMaxColumn) {
        tempMaxColumn = column;
      }
    }
    maxIndex = tempMaxColumn;
    maxColumn = tempMaxColumn + 1;
    positions = new int[maxColumn + 1];
    this.srcSeparator = srcSeparator;
    this.dstSeparators = dstSeparators;
    this.prefix = prefix;
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
    boolean separatorAtEnd = true;
    if (i == length && index < maxColumn) {
      nextVirginIndex = index + 2;
      positions[nextVirginIndex - 1] = length;
      separatorAtEnd = length > 0 ? body[length - 1] == srcSeparator : false;
    } else {
      nextVirginIndex = index + 1;
    }

    int newArrayLen = prefix.length;
    for (i = columns.length; i-- > 0; ) {
      int column = columns[i];
      int len = positions[column + 1] - positions[column];
      if (len > 0) {
        if (positions[column + 1] == length && !separatorAtEnd) {
          newArrayLen += len;
        } else {
          newArrayLen += len - 1;
        }
      }
      newArrayLen += dstSeparators[i].length;
    }

    byte[] newBody = new byte[newArrayLen];
    int newOffset = 0;
    if (prefix.length > 0) {
      System.arraycopy(prefix, 0, newBody, 0, prefix.length);
      newOffset += prefix.length;
    }
    int dstSeparatorsIdx = 0;
    for (int column : columns) {
      int len = positions[column + 1] - positions[column];
      byte[] separator = dstSeparators[dstSeparatorsIdx++];
      if (len > 0) {
        System.arraycopy(body, positions[column], newBody, newOffset, len);
        newOffset += len;
        if (newBody[newOffset - 1] == srcSeparator) {
          newOffset--;
        }
      }
      System.arraycopy(separator, 0, newBody, newOffset, separator.length);
      newOffset += separator.length;
    }
    event.setBody(newBody);
    Arrays.fill(positions, 1, nextVirginIndex, 0);
    return event;
  }

  @Override
  public List<Event> intercept(List<Event> events)
  {
    for (Event event : events) {
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
    private byte[][] dstSeparators;
    private byte[] prefix;

    @Override
    public Interceptor build()
    {
      return new ColumnFilteringFormattingInterceptor(columns, srcSeparator, dstSeparators, prefix);
    }

    @Override
    public void configure(Context context)
    {
      String formatter = context.getString(COLUMNS_FORMATTER);
      if (Strings.isNullOrEmpty(formatter)) {
        throw new IllegalArgumentException("This interceptor requires columns format to be specified!");
      }
      List<String> lSeparators = Lists.newArrayList();
      List<Integer> lColumns = Lists.newArrayList();
      Pattern colPat = Pattern.compile("\\{\\d+?\\}");
      Matcher matcher = colPat.matcher(formatter);
      int separatorStart = 0;
      String lPrefix = "";
      while (matcher.find()) {
        String col = matcher.group();
        lColumns.add(Integer.parseInt(col.substring(1, col.length() - 1)));
        if (separatorStart == 0 && matcher.start() > 0) {
          lPrefix = formatter.substring(0, matcher.start());
        } else if (separatorStart > 0) {
          lSeparators.add(formatter.substring(separatorStart, matcher.start()));
        }

        separatorStart = matcher.end();
      }
      if (separatorStart < formatter.length()) {
        lSeparators.add(formatter.substring(separatorStart, formatter.length()));
      }
      columns = Ints.toArray(lColumns);
      byte[] emptyStringBytes = "".getBytes();

      dstSeparators = new byte[columns.length][];

      for (int i = 0; i < columns.length; i++) {
        if (i < lSeparators.size()) {
          dstSeparators[i] = lSeparators.get(i).getBytes();
        } else {
          dstSeparators[i] = emptyStringBytes;
        }
      }
      srcSeparator = context.getInteger(SRC_SEPARATOR, (int)SRC_SEPARATOR_DFLT).byteValue();
      this.prefix = lPrefix.getBytes();
    }
  }

  public static class Constants extends ColumnFilteringInterceptor.Constants
  {
    public static final String COLUMNS_FORMATTER = "columnsFormatter";
  }

  private static final Logger logger = LoggerFactory.getLogger(ColumnFilteringFormattingInterceptor.class);

}
