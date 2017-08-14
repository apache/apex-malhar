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
package org.apache.apex.malhar.hive;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.fs.AbstractFileOutputOperator;
import org.apache.commons.lang3.mutable.MutableInt;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.CheckpointNotificationListener;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * An implementation of FS Writer that writes text files to hdfs which are
 * inserted into hive on committed window callback. HiveStreamCodec is used to
 * make sure that data being sent to a particular hive partition goes to a
 * specific operator partition by passing FSRollingOutputOperator to the stream
 * codec. Also filename is determined uniquely for each tuple going to a
 * specific hive partition.
 *
 * @since 2.1.0
 */
public abstract class AbstractFSRollingOutputOperator<T> extends AbstractFileOutputOperator<T> implements CheckpointNotificationListener
{
  private transient String outputFilePath;
  protected MutableInt partNumber;
  protected HashMap<Long, ArrayList<String>> mapFilenames = new HashMap<Long, ArrayList<String>>();
  protected HashMap<String, ArrayList<String>> mapPartition = new HashMap<String, ArrayList<String>>();
  protected Queue<Long> queueWindows = new LinkedList<Long>();
  protected long windowIDOfCompletedPart = Stateless.WINDOW_ID;
  protected long committedWindowId = Stateless.WINDOW_ID;
  private boolean isEmptyWindow;
  private transient int operatorId;
  private int countEmptyWindow;
  private ArrayList<String> partition = new ArrayList<String>();

  //This variable is user configurable.
  @Min(0)
  private long maxWindowsWithNoData = 100;

  /**
   * The output port that will emit a POJO containing file which is committed
   * and specific hive partitions in which this file should be loaded to
   * HiveOperator.
   */
  public final transient DefaultOutputPort<FilePartitionMapping> outputPort = new DefaultOutputPort<FilePartitionMapping>();

  public AbstractFSRollingOutputOperator()
  {
    countEmptyWindow = 0;
    HiveStreamCodec<T> hiveCodec = new HiveStreamCodec<T>();
    hiveCodec.rollingOperator = this;
    streamCodec = hiveCodec;
  }

  @Override
  public void setup(OperatorContext context)
  {
    String appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();
    outputFilePath = File.separator + appId + File.separator + operatorId;
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId)
  {
    isEmptyWindow = true;
    windowIDOfCompletedPart = windowId;
  }

  /*
   * MapFilenames has mapping from completed file to windowId in which it got completed.
   * Also maintaining a queue of these windowIds which helps in reducing iteration time in committed callback.
   */
  @Override
  protected void rotateHook(String finishedFile)
  {
    isEmptyWindow = false;
    if (mapFilenames.containsKey(windowIDOfCompletedPart)) {
      mapFilenames.get(windowIDOfCompletedPart).add(finishedFile);
    } else {
      ArrayList<String> listFileNames = new ArrayList<String>();
      listFileNames.add(finishedFile);
      mapFilenames.put(windowIDOfCompletedPart, listFileNames);
    }
    queueWindows.add(windowIDOfCompletedPart);

  }

  /*
   * Filenames include operator Id and the specific hive partitions to which the file will be loaded.
   * Partition is determined based on tuple and its implementation is left to user.
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
        logger.debug("list is {}", mapFilenames.get(windowId));
        list = mapFilenames.get(windowId);
        FilePartitionMapping partMap = new FilePartitionMapping();
        if (list != null) {
          for (int i = 0; i < list.size(); i++) {
            partMap.setFilename(list.get(i));
            partMap.setPartition(mapPartition.get(list.get(i)));
            outputPort.emit(partMap);
          }
        }
        mapFilenames.remove(windowId);
        iterWindows.remove();
      }
      if (committedWindowId < windowId) {
        break;
      }
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
    } catch (IOException ex) {
      logger.debug(ex.getMessage());
      DTThrowable.rethrow(ex);
    } catch (ExecutionException ex) {
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
    return getPartFileName(lastFile, partNumber.intValue());
  }

  /**
   * This method gets a List of Hive Partitions in which the tuple needs to be
   * written to. Example: If hive partitions are date='2014-12-12',country='USA'
   * then this method returns {"2014-12-12","USA"} The implementation is left to
   * the user.
   *
   * @param tuple
   *          A received tuple to be written to a hive partition.
   * @return ArrayList containing hive partition values.
   */
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
    super.endWindow();

  }

  public long getMaxWindowsWithNoData()
  {
    return maxWindowsWithNoData;
  }

  public void setMaxWindowsWithNoData(long maxWindowsWithNoData)
  {
    this.maxWindowsWithNoData = maxWindowsWithNoData;
  }

  /*
   * A POJO which is emitted by output port of AbstractFSRollingOutputOperator implementation in DAG.
   * The POJO contains the filename which will not be changed by FSRollingOutputOperator once its emitted.
   * The POJO also contains the hive partitions to which the respective files will be moved.
   */
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

  private static final Logger logger = LoggerFactory.getLogger(AbstractFSRollingOutputOperator.class);

}
