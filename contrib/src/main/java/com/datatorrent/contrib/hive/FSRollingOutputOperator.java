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
package com.datatorrent.contrib.hive;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.common.util.DTThrowable;

/*
 * An implementation of FS Writer that writes text files to hdfs which are inserted
 * into hive on committed window callback.
 */
public abstract class FSRollingOutputOperator<T> extends AbstractFileOutputOperator<T> implements CheckpointListener
{
  private transient String outputFilePath;
  protected MutableInt partNumber;
  protected HashMap<Long, ArrayList<String>> mapFilenames = new HashMap<Long, ArrayList<String>>();
  protected ArrayList<String> listFileNames = new ArrayList<String>();
  protected HashMap<String, ArrayList<String>> mapPartition = new HashMap<String, ArrayList<String>>();
  protected TreeSet<Long> queueWindows = new TreeSet<Long>();
  // Hdfs block size which can be set as a property by user.
  private static final int MAX_LENGTH = 66060288;
  protected long windowIDOfCompletedPart = Stateless.WINDOW_ID;
  protected long committedWindowId = Stateless.WINDOW_ID;
  private transient boolean isEmptyWindow;
  private transient int operatorId;
  private int countEmptyWindow;
  private ArrayList<String> partition = new ArrayList<String>();

  //This variable is user configurable.
  @Min(0)
  private long maxWindowsWithNoData = 100;
  @NotNull
  private Converter<T> converter;

  /**
   * The output port that will emit tuple into DAG.
   */
  public final transient DefaultOutputPort<FilePartitionMapping> outputPort = new DefaultOutputPort<FilePartitionMapping>();

  public FSRollingOutputOperator()
  {
    countEmptyWindow = 0;
    setMaxLength(MAX_LENGTH);
    HiveStreamCodec<T> streamCodec = new HiveStreamCodec<T>();
    streamCodec.rollingOperator = this;
    this.setStreamCodec(streamCodec);
  }

  @Override
  public void setup(OperatorContext context)
  {
    String appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();
    outputFilePath = File.separator + appId + File.separator + operatorId;
    isEmptyWindow = true;
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    isEmptyWindow = true;
    windowIDOfCompletedPart = windowId;
  }

  /*
   * To be implemented by the user, giving a default implementation for one partition column here.
   * Partitions array will get all hive partitions in it.
   * MapFilenames has mapping from completed file to windowId in which it got completed.
   *
   */
  @Override
  protected void rotateHook(String finishedFile)
  {
    if (mapFilenames.containsKey(windowIDOfCompletedPart)) {
      listFileNames.add(finishedFile);
    }
    else {
      listFileNames = new ArrayList<String>();
      listFileNames.add(finishedFile);
    }
    mapFilenames.put(windowIDOfCompletedPart, listFileNames);

    queueWindows.add(windowIDOfCompletedPart);
  }

  /*
   * To be implemented by the user, giving a default implementation for one partition column here.
   */
  @Override
  protected String getFileName(T tuple)
  {
    isEmptyWindow = false;
    partition = getHivePartition(tuple);
    StringBuilder output = new StringBuilder(outputFilePath);
    int numPartitions = partition.size();
    if (numPartitions > 0) {
      for (int i = 0; i < numPartitions; i++) {
        output.append(File.separator).append(partition.get(i));
      }
      output.append(File.separator).append(operatorId).append("-transaction.out.part");
      String partFile = getPartFileNamePri(output.toString());
      mapPartition.put(partFile, partition);
    }
    return output.toString();
  }

  /*
   * Implement this function according to tuple you want to pass in Hive.
   */
  @Override
  protected byte[] getBytesForTuple(T tuple)
  {
    String output = converter.getTuple(tuple);
    return output.getBytes();
  }

  /*
   * Moving completed files into hive on committed window callback.
   * Criteria for moving them is that the windowId in which they are completed
   * should be less than committed window.
   */
  @Override
  public void committed(long windowId)
  {
    committedWindowId = windowId;
    Iterator<Long> iterWindows = queueWindows.iterator();
    ArrayList<String> list = new ArrayList<String>();
    while (iterWindows.hasNext()) {
      windowId = iterWindows.next();
      if (committedWindowId >= windowId) {
        list = mapFilenames.get(windowId);
      }
      FilePartitionMapping partMap = new FilePartitionMapping();
      for (int i = 0; i < list.size(); i++) {
        partMap.setFilename(list.get(i));
        partMap.setPartition(mapPartition.get(list.get(i)));
        outputPort.emit(partMap);
      }
      mapFilenames.remove(windowId);
      iterWindows.remove();
    }
  }

  @Override
  public void checkpointed(long windowId)
  {
  }

  protected void rotateCall(String lastFile)
  {
    try {
      this.rotate(lastFile);
    }
    catch (IOException ex) {
      logger.debug(ex.getMessage());
      DTThrowable.rethrow(ex);
    }
    catch (ExecutionException ex) {
      logger.debug(ex.getMessage());
      DTThrowable.rethrow(ex);
    }
  }

  public String getHDFSRollingLastFile()
  {
    Iterator<String> iterFileNames = this.openPart.keySet().iterator();
    String lastFile = null;
    if (iterFileNames.hasNext()) {
      lastFile = iterFileNames.next();
      partNumber = this.openPart.get(lastFile);
    }
    return getPartFileName(lastFile,
                           partNumber.intValue());
  }

  public abstract ArrayList<String> getHivePartition(T tuple);

  @Override
  public void endWindow()
  {
    if (isEmptyWindow) {
      countEmptyWindow++;
    }
    if (countEmptyWindow >= maxWindowsWithNoData) {
      String lastFile = getHDFSRollingLastFile();
      rotateCall(lastFile);
      countEmptyWindow = 0;
    }

  }

  public long getMaxWindowsWithNoData()
  {
    return maxWindowsWithNoData;
  }

  public void setMaxWindowsWithNoData(long maxWindowsWithNoData)
  {
    this.maxWindowsWithNoData = maxWindowsWithNoData;
  }

  public Converter<T> getConverter()
  {
    return converter;
  }

  public void setConverter(Converter<T> converter)
  {
    this.converter = converter;
  }

  public static class FilePartitionMapping
  {
    private String filename;
    private ArrayList<String> partition = new ArrayList<String>();

    public ArrayList<String> getPartition()
    {
      return partition;
    }

    public void setPartition(ArrayList<String> partition)
    {
      this.partition = partition;
    }

    public String getFilename()
    {
      return filename;
    }

    public void setFilename(String filename)
    {
      this.filename = filename;
    }

  }

  private static final Logger logger = LoggerFactory.getLogger(FSRollingOutputOperator.class);

}
