/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.source;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

public class TestSource extends AbstractSource implements EventDrivenSource, Configurable
{
  static String FILE_NAME = "sourceFile";
  static String RATE = "rate";
  static byte FIELD_SEPARATOR = 1;
  private static String d1 = "2013-11-07";
  public Timer emitTimer;
  @Nonnull
  String filePath;
  int rate;
  transient List<Event> cache;
  private transient int startIndex;

  public TestSource()
  {
    super();
    this.rate = 2500;
  }

  @Override
  public void configure(Context context)
  {
    filePath = context.getString(FILE_NAME);
    rate = context.getInteger(RATE, rate);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(filePath));
    try {
      BufferedReader lineReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
      try {
        buildCache(lineReader);
      }
      finally {
        lineReader.close();
      }
    }
    catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start()
  {
    super.start();
    emitTimer = new Timer();

    final ChannelProcessor channel = getChannelProcessor();
    final int cacheSize = cache.size();
    emitTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        int lastIndex = startIndex + rate;
        if (lastIndex > cacheSize) {
          lastIndex -= cacheSize;
          channel.processEventBatch(cache.subList(startIndex, cacheSize));
          while (lastIndex > cacheSize) {
            channel.processEventBatch(cache);
            lastIndex -= cacheSize;
          }
          channel.processEventBatch(cache.subList(0, lastIndex));
        }
        else {
          channel.processEventBatch(cache.subList(startIndex, lastIndex));
        }
        startIndex = lastIndex;
      }

    }, 0, 1000);
  }

  @Override
  public void stop()
  {
    emitTimer.cancel();
    super.stop();
  }

  private void buildCache(BufferedReader lineReader) throws IOException
  {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    Calendar cal = Calendar.getInstance();
    String todayDate = format.format(cal.getTimeInMillis());

    cal.add(Calendar.DATE, -1);
    String yesterdayDate = format.format(cal.getTime());

    cache = Lists.newArrayListWithCapacity(rate);

    String line;
    while ((line = lineReader.readLine()) != null) {
      byte[] row = line.getBytes();
      final int rowsize = row.length;

      /* guid */
      int sliceLengh = -1;
      while (++sliceLengh < rowsize) {
        if (row[sliceLengh] == FIELD_SEPARATOR) {
          break;
        }
      }
      /* skip the next record */
      int pointer = sliceLengh + 1;
      while (pointer < rowsize) {
        if (row[pointer++] == FIELD_SEPARATOR) {
          break;
        }
      }

      /* lets parse the date */
      int dateStart = pointer;
      while (pointer < rowsize) {
        if (row[pointer++] == FIELD_SEPARATOR) {
          String date = new String(row, dateStart, pointer - dateStart - 1);
          if (date.indexOf(d1) >= 0) {
            System.arraycopy(yesterdayDate.getBytes(), 0, row, dateStart, yesterdayDate.getBytes().length);
          }
          else {
            System.arraycopy(todayDate.getBytes(), 0, row, dateStart, todayDate.getBytes().length);
          }
          break;
        }
      }
      cache.add(EventBuilder.withBody(row));
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(TestSource.class);
}
