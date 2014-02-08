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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class TestSource extends AbstractSource implements EventDrivenSource, Configurable
{
  private static final Logger logger = LoggerFactory.getLogger(TestSource.class);
  static String FILE_NAME = "sourceFile";
  static String RATE = "rate";
  static byte FIELD_SEPARATOR = 1;
  private static String d1 = "2013-11-07";
  public Timer emitTimer;
  @Nonnull
  public String filePath;
  public int rate;
  public transient String yesterdayDate;
  public transient String todayDate;
  private transient List<Event> cache;
  private transient int startIdx;

  public TestSource()
  {
    super();
    this.rate = 2500;
    startIdx = 0;
  }

  public void updateDates()
  {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    Calendar cal = Calendar.getInstance();
    todayDate = format.format(cal.getTimeInMillis());

    cal.add(Calendar.DATE, -1);
    yesterdayDate = format.format(cal.getTime());
  }

  @Override
  public void configure(Context context)
  {
    filePath = context.getString(FILE_NAME);
    rate = context.getInteger(RATE, rate);
    emitTimer = new Timer();
    Preconditions.checkArgument(!Strings.isNullOrEmpty(filePath));
    cache = Lists.newArrayListWithCapacity(rate);
    try {
      BufferedReader lineReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
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
        logger.debug(new String(row, 0, row.length));
        cache.add(EventBuilder.withBody(row));
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

    final ChannelProcessor channel = getChannelProcessor();
    emitTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        int size = cache.size();
        int index = startIdx;

        try {
          for (int i = rate; i-- > 0;) {
            channel.processEvent(cache.get(index));
            index = (index + 1) % size;
          }
        }
        finally {
          startIdx = index;
        }
      }

    }, 0, 1000);
  }

  @Override
  public void stop()
  {
    emitTimer.cancel();
    super.stop();
  }

}
