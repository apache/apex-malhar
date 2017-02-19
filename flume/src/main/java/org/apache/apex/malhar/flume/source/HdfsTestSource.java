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
package org.apache.apex.malhar.flume.source;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * <p>TestSource class.</p>
 *
 * @since 0.9.4
 */
public class HdfsTestSource extends AbstractSource implements EventDrivenSource, Configurable
{
  public static final String SOURCE_DIR = "sourceDir";
  public static final String RATE = "rate";
  public static final String INIT_DATE = "initDate";

  static byte FIELD_SEPARATOR = 2;
  public Timer emitTimer;
  @Nonnull
  String directory;
  Path directoryPath;
  int rate;
  String initDate;
  long initTime;
  List<String> dataFiles;
  long oneDayBack;

  private transient BufferedReader br = null;
  protected transient FileSystem fs;
  private transient Configuration configuration;

  private transient int currentFile = 0;
  private transient boolean finished;
  private List<Event> events;

  public HdfsTestSource()
  {
    super();
    this.rate = 2500;
    dataFiles = Lists.newArrayList();
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, -1);
    oneDayBack = calendar.getTimeInMillis();
    configuration = new Configuration();
    events = Lists.newArrayList();
  }

  @Override
  public void configure(Context context)
  {
    directory = context.getString(SOURCE_DIR);
    rate = context.getInteger(RATE, rate);
    initDate = context.getString(INIT_DATE);

    Preconditions.checkArgument(!Strings.isNullOrEmpty(directory));
    directoryPath = new Path(directory);

    String[] parts = initDate.split("-");
    Preconditions.checkArgument(parts.length == 3);
    Calendar calendar = Calendar.getInstance();
    calendar.set(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]) - 1, Integer.parseInt(parts[2]), 0, 0, 0);
    initTime = calendar.getTimeInMillis();

    try {
      List<String> files = findFiles();
      for (String file : files) {
        dataFiles.add(file);
      }
      if (logger.isDebugEnabled()) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        logger.debug("settings {} {} {} {} {}", directory, rate, dateFormat.format(oneDayBack),
            dateFormat.format(new Date(initTime)), currentFile);
        for (String file : dataFiles) {
          logger.debug("settings add file {}", file);
        }
      }

      fs = FileSystem.newInstance(new Path(directory).toUri(), configuration);
      Path filePath = new Path(dataFiles.get(currentFile));
      br = new BufferedReader(new InputStreamReader(new GzipCompressorInputStream(fs.open(filePath))));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    finished = true;

  }

  private List<String> findFiles() throws IOException
  {
    List<String> files = Lists.newArrayList();
    Path directoryPath = new Path(directory);
    FileSystem lfs = FileSystem.newInstance(directoryPath.toUri(), configuration);
    try {
      logger.debug("checking for new files in {}", directoryPath);
      RemoteIterator<LocatedFileStatus> statuses = lfs.listFiles(directoryPath, true);
      for (; statuses.hasNext(); ) {
        FileStatus status = statuses.next();
        Path path = status.getPath();
        String filePathStr = path.toString();
        if (!filePathStr.endsWith(".gz")) {
          continue;
        }
        logger.debug("new file {}", filePathStr);
        files.add(path.toString());
      }
    } catch (FileNotFoundException e) {
      logger.warn("Failed to list directory {}", directoryPath, e);
      throw new RuntimeException(e);
    } finally {
      lfs.close();
    }
    return files;
  }

  @Override
  public void start()
  {
    super.start();
    emitTimer = new Timer();

    final ChannelProcessor channelProcessor = getChannelProcessor();
    emitTimer.scheduleAtFixedRate(new TimerTask()
    {
      @Override
      public void run()
      {
        int lineCount = 0;
        events.clear();
        try {
          while (lineCount < rate && !finished) {
            String line = br.readLine();

            if (line == null) {
              logger.debug("completed file {}", currentFile);
              br.close();
              currentFile++;
              if (currentFile == dataFiles.size()) {
                logger.info("finished all files");
                finished = true;
                break;
              }
              Path filePath = new Path(dataFiles.get(currentFile));
              br = new BufferedReader(new InputStreamReader(new GzipCompressorInputStream(fs.open(filePath))));
              logger.info("opening file {}. {}", currentFile, filePath);
              continue;
            }
            lineCount++;
            Event flumeEvent = EventBuilder.withBody(line.getBytes());
            events.add(flumeEvent);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        if (events.size() > 0) {
          channelProcessor.processEventBatch(events);
        }
        if (finished) {
          emitTimer.cancel();
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

  private static final Logger logger = LoggerFactory.getLogger(HdfsTestSource.class);
}
