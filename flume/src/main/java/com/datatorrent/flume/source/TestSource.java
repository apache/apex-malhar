/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.source;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * <p>TestSource class.</p>
 *
 * @since 0.9.4
 */
public class TestSource extends AbstractSource implements EventDrivenSource, Configurable
{
  public static final String SOURCE_FILE = "sourceFile";
  public static final String LINE_NUMBER = "lineNumber";
  public static final String RATE = "rate";
  public static final String PERCENT_PAST_EVENTS = "percentPastEvents";
  static byte FIELD_SEPARATOR = 1;
  static int DEF_PERCENT_PAST_EVENTS = 5;
  public Timer emitTimer;
  @Nonnull
  String filePath;
  int rate;
  int numberOfPastEvents;
  transient List<Row> cache;
  private transient int startIndex;
  private transient Random random;
  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
  private SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public TestSource()
  {
    super();
    this.rate = 2500;
    this.numberOfPastEvents = DEF_PERCENT_PAST_EVENTS * 25;
    this.random = new Random();

  }

  @Override
  public void configure(Context context)
  {
    filePath = context.getString(SOURCE_FILE);
    rate = context.getInteger(RATE, rate);
    int percentPastEvents = context.getInteger(PERCENT_PAST_EVENTS, DEF_PERCENT_PAST_EVENTS);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(filePath));
    try {
      BufferedReader lineReader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
      try {
        buildCache(lineReader);
      } finally {
        lineReader.close();
      }
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (DEF_PERCENT_PAST_EVENTS != percentPastEvents) {
      numberOfPastEvents = (int)(percentPastEvents / 100.0 * cache.size());
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
          processBatch(channel, cache.subList(startIndex, cacheSize));
          startIndex = 0;
          while (lastIndex > cacheSize) {
            processBatch(channel, cache);
            lastIndex -= cacheSize;
          }
          processBatch(channel, cache.subList(0, lastIndex));
        } else {
          processBatch(channel, cache.subList(startIndex, lastIndex));
        }
        startIndex = lastIndex;
      }

    }, 0, 1000);
  }

  private void processBatch(ChannelProcessor channelProcessor, List<Row> rows)
  {
    if (rows.isEmpty()) {
      return;
    }

    int noise = random.nextInt(numberOfPastEvents + 1);
    Set<Integer> pastIndices = Sets.newHashSet();
    for (int i = 0; i < noise; i++) {
      pastIndices.add(random.nextInt(rows.size()));
    }

    Calendar calendar = Calendar.getInstance();
    long high = calendar.getTimeInMillis();
    calendar.add(Calendar.DATE, -2);
    long low = calendar.getTimeInMillis();



    List<Event> events = Lists.newArrayList();
    for (int i = 0; i < rows.size(); i++) {
      Row eventRow = rows.get(i);
      if (pastIndices.contains(i)) {
        long pastTime = (long)((Math.random() * (high - low)) + low);
        byte[] pastDateField = dateFormat.format(pastTime).getBytes();
        byte[] pastTimeField = timeFormat.format(pastTime).getBytes();

        System.arraycopy(pastDateField, 0, eventRow.bytes, eventRow.dateFieldStart, pastDateField.length);
        System.arraycopy(pastTimeField, 0, eventRow.bytes, eventRow.timeFieldStart, pastTimeField.length);
      } else {
        calendar.setTimeInMillis(System.currentTimeMillis());
        byte[] currentDateField = dateFormat.format(calendar.getTime()).getBytes();
        byte[] currentTimeField = timeFormat.format(calendar.getTime()).getBytes();

        System.arraycopy(currentDateField, 0, eventRow.bytes, eventRow.dateFieldStart, currentDateField.length);
        System.arraycopy(currentTimeField, 0, eventRow.bytes, eventRow.timeFieldStart, currentTimeField.length);
      }

      HashMap<String, String> headers = new HashMap<String, String>(2);
      headers.put(SOURCE_FILE, filePath);
      headers.put(LINE_NUMBER, String.valueOf(startIndex + i));
      events.add(EventBuilder.withBody(eventRow.bytes, headers));
    }
    channelProcessor.processEventBatch(events);
  }

  @Override
  public void stop()
  {
    emitTimer.cancel();
    super.stop();
  }

  private void buildCache(BufferedReader lineReader) throws IOException
  {
    cache = Lists.newArrayListWithCapacity(rate);

    String line;
    while ((line = lineReader.readLine()) != null) {
      byte[] row = line.getBytes();
      Row eventRow = new Row(row);
      final int rowsize = row.length;

      /* guid */
      int sliceLengh = -1;
      while (++sliceLengh < rowsize) {
        if (row[sliceLengh] == FIELD_SEPARATOR) {
          break;
        }
      }
      int recordStart = sliceLengh + 1;
      int pointer = sliceLengh + 1;
      while (pointer < rowsize) {
        if (row[pointer++] == FIELD_SEPARATOR) {
          eventRow.dateFieldStart = recordStart;
          break;
        }
      }

      /* lets parse the date */
      int dateStart = pointer;
      while (pointer < rowsize) {
        if (row[pointer++] == FIELD_SEPARATOR) {
          eventRow.timeFieldStart = dateStart;
          break;
        }
      }

      cache.add(eventRow);
    }
  }

  private static class Row
  {
    final byte[] bytes;
    int dateFieldStart;
    int timeFieldStart;
//    boolean past;

    Row(byte[] bytes)
    {
      this.bytes = bytes;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(TestSource.class);
}
