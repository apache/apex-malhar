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

import java.sql.SQLException;
import java.sql.Statement;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.AbstractStoreOutputOperator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.CheckpointListener;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;

import com.datatorrent.common.util.DTThrowable;
import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.fs.FileSystem;

/*
 * An abstract Hive operator which can insert data in ORC/TEXT tables from a file written in hdfs location.
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class AbstractHiveHDFS<T, S extends HiveStore> extends AbstractStoreOutputOperator<T, HiveStore> implements CheckpointListener
{
  protected long committedWindowId = Stateless.WINDOW_ID;
  //This variable can be used for debugging purposes
  private long checkpointedWindowId = Stateless.WINDOW_ID;
  private static final Logger logger = LoggerFactory.getLogger(AbstractHiveHDFS.class);
  private transient String appId;
  private transient int operatorId;
  protected HashMap<String, Long> filenames;
  //This variable is user configurable
  private transient long maxWindowsWithNoData = 100;
  private int countEmptyWindow;
  private transient boolean isEmptyWindow;

  protected Long maxLength = Long.MAX_VALUE;

  @Nonnull
  protected String tablename;

  public HDFSRollingOutputOperator hdfsOp;

  public AbstractHiveHDFS()
  {
    hdfsOp = new HDFSRollingOutputOperator();
    filenames = new HashMap<String, Long>();
    hdfsOp.hive = this;
    countEmptyWindow = 0;
  }

  public Long getMaxLength()
  {
    return hdfsOp.getMaxLength();
  }

  public void setMaxLength(Long maxLength)
  {
    hdfsOp.setMaxLength(maxLength);
  }

  public int getPermission()
  {
    return hdfsOp.getFsPermission();
  }

  public void setPermission(int permission)
  {
    hdfsOp.setFsPermission(permission);
  }

  @Override
  public void committed(long windowId)
  {
    committedWindowId = windowId;
    logger.info("committedwindow is {}, now send tuples to hive" + committedWindowId);
    Iterator<String> iter = filenames.keySet().iterator();
    while (iter.hasNext()) {
      String fileMoved = iter.next();
      long window = filenames.get(fileMoved);
      logger.info("windowid is " + window);
      if (committedWindowId >= window) {
        processHiveFile(fileMoved);
        logger.info("remove this file from map" + fileMoved);
        iter.remove();
      }
    }
  }

  @Override
  public void checkpointed(long windowId)
  {
    checkpointedWindowId = windowId;
  }

  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  /**
   * Function to process each incoming tuple
   *
   * @param tuple incoming tuple
   */
  @Override
  public void processTuple(T tuple)
  {
    isEmptyWindow = false;
    logger.info("writing to file");
    hdfsOp.input.process(tuple);
  }

  @Override
  public void setup(OperatorContext context)
  {
    appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();
    hdfsOp.setFilePath(store.filepath + "/" + appId + "/" + operatorId);
    store.setOperatorpath(store.filepath + "/" + appId + "/" + operatorId);
    logger.info("Filepath is" + store.filepath);
    logger.info("operator path is" + store.operatorpath);
    super.setup(context);
    hdfsOp.setup(context);
    logger.info("committed window id from callback is {} " + committedWindowId);
    logger.debug("AppId {} OperatorId {}", appId, operatorId);
    isEmptyWindow = true;
  }

  @Override
  public void teardown()
  {
    hdfsOp.teardown();
    super.teardown();
  }

  @Override
  public void beginWindow(long windowId)
  {
    isEmptyWindow = true;
    hdfsOp.beginWindow(windowId);
    logger.debug("committed window from callback {}", committedWindowId);
  }

  @Override
  public void endWindow()
  {
    hdfsOp.endWindow();
    if (isEmptyWindow) {
      countEmptyWindow++;
    }
    if (countEmptyWindow >= maxWindowsWithNoData) {
      File f = new File(store.operatorpath + "/" + hdfsOp.lastFile);
      if (f.exists()) {
        logger.info("last file not moved");
        processHiveFile(hdfsOp.lastFile);
      }
      hdfsOp.partNumber.increment();
      hdfsOp.updatePartNumber();
    }
  }

  public void processHiveFile(String fileMoved)
  {
    logger.info("in process hive file");
    logger.info("processing {} file", fileMoved);
    String command = getInsertCommand(store.getOperatorpath() + "/" + fileMoved);
    logger.info("command is" + command);
    Statement stmt;
    try {
      stmt = store.getConnection().createStatement();
      stmt.execute(command);
    }
    catch (SQLException ex) {
      logger.info(AbstractHiveHDFS.class.getName() + " " + ex.getMessage());
      DTThrowable.rethrow(ex);
    }
  }

  public String getHiveTuple(T tuple)
  {
    return tuple.toString() + "\n";
  }

  protected String getInsertCommand(String filepath)
  {
    String command = null;
    logger.info("filepath is {}",filepath);
    if(!hdfsOp.isHDFSLocation())
    command = "load data inpath '" + filepath + "'OVERWRITE into table " + tablename;
    else
    command = "load data local inpath '" + filepath + "'OVERWRITE into table " + tablename;
    logger.info("command is {}" + command);

    return command;

  }

}
