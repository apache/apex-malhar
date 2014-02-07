/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.source;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class DTFlumeSource extends AbstractSource implements EventDrivenSource, Configurable
{
  static String FILE_NAME = "sourceFile";
  static String RATE = "rate";
  private static String d1 = "2013-11-07";
  private static String d2 = "2103-11-08";

  public Timer emitTimer;

  @Nonnull
  public String filePath;
  public int rate;
  public String yesterdayDate;
  public String todayDate;

  private transient BufferedReader lineReader;

  public DTFlumeSource()
  {
    super();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    Calendar cal = Calendar.getInstance();
    todayDate = format.format(cal.getTime());
    cal.add(Calendar.DATE, -1);
    yesterdayDate = format.format(cal.getTime());
    rate = -1;

  }

  @Override
  public void configure(Context context)
  {
    filePath = Preconditions.checkNotNull(context.getString(FILE_NAME));
    rate = context.getInteger(RATE);
    emitTimer = new Timer();
    Preconditions.checkArgument(!Strings.isNullOrEmpty(filePath));
    try {
      lineReader = new BufferedReader(new InputStreamReader( new FileInputStream(filePath)));
    }
    catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void start()
  {
    final ChannelProcessor channel = getChannelProcessor();
    emitTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        try {
          for (int i = 0; i < rate; i++) {
            String line = lineReader.readLine();
            if (line == null) {
              lineReader.close();
              lineReader = new BufferedReader(new InputStreamReader( new FileInputStream(filePath)));
              line = lineReader.readLine();
            }
            String eventToSend = null;
            if (line.contains(d1)) {
              eventToSend = line.replaceAll(d1, yesterdayDate);
            }
            if (line.contains(d2)) {
              eventToSend = line.replaceAll(d2, todayDate);
            }
            Event event = EventBuilder.withBody(eventToSend.getBytes());
            channel.processEvent(event);
          }
        }
        catch (IOException e) {
          throw new RuntimeException(e);
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
