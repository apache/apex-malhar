/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.flume.source;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
  private static String FILE_NAME = "sourceFile";
  private static String RATE = "rate";
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
      lineReader = new BufferedReader(new FileReader(filePath));
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
              lineReader = new BufferedReader(new FileReader(filePath));
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
