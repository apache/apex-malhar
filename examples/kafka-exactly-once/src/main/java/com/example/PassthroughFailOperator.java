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
package com.example;

import java.io.IOException;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * To produce an exactly-once scenario the PassthroughFailOperator kills itself after a certain number
 * of processed lines by throwing an exception. YARN will deploy the Operator in a new container,
 * hence not checkpointed tuples will be passed to the OutputOperators more than once.
 */
public class PassthroughFailOperator extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(PassthroughFailOperator.class);
  private boolean killed;

  @NotNull
  private int tuplesUntilKill;

  //start with empty windows to ensure tests run reliable
  private int emptyWindowsCount = 0;

  @NotNull
  private String directoryPath;

  private String filePath;
  private transient FileSystem hdfs;
  private transient Path filePathObj;

  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

  /**
   * Loads file from HDFS and sets {@link #killed} flag if it already exists
   *
   * @param context
   */
  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    String appId = context.getValue(Context.DAGContext.APPLICATION_ID);
    filePath = directoryPath + "/" + appId;

    LOG.info("FilePath: " + filePath);
    filePathObj = new Path(filePath);
    try {
      hdfs = FileSystem.newInstance(filePathObj.toUri(), new Configuration());
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      if (hdfs.exists(filePathObj)) {
        killed = true;
        LOG.info("file already exists -> Operator has been killed before");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    LOG.info("WindowId: " + windowId);
    ++emptyWindowsCount;
  }

  public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
  {
    /**
     * Creates file on HDFS identified by ApplicationId to save killed state, if operator has not been killed yet.
     * Throws Exception to kill operator.
     *
     * @param line
     */
    @Override
    public void process(String line)
    {
      if (emptyWindowsCount > 5) {
        LOG.info("LINE " + line);
        if (killed) {
          output.emit(line);
        } else if (tuplesUntilKill > 0) {
          output.emit(line);
          tuplesUntilKill--;
        } else {
          try {
            hdfs.createNewFile(filePathObj);
            LOG.info("Created file " + filePath);
          } catch (IOException e) {
            e.printStackTrace();
          }
          //kill operator
          LOG.info("Operator intentionally killed through exception");
          RuntimeException e = new RuntimeException("Exception to intentionally kill operator");
          throw e;
        }
      }
    }
  };

  public String getDirectoryPath()
  {
    return directoryPath;
  }

  public void setDirectoryPath(String directoryPath)
  {
    this.directoryPath = directoryPath;
  }

  public int getTuplesUntilKill()
  {
    return tuplesUntilKill;
  }

  public void setTuplesUntilKill(int tuplesUntilKill)
  {
    this.tuplesUntilKill = tuplesUntilKill;
  }

}

