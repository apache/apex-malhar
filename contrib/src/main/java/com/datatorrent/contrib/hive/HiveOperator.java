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
import java.util.*;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.AbstractStoreOutputOperator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.contrib.hive.FSRollingOutputOperator.FilePartitionMapping;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * Hive operator which can insert data in txt format in tables/partitions from a file written in hdfs location.
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class HiveOperator extends AbstractStoreOutputOperator<FilePartitionMapping, HiveStore>
{
  //This Property is user configurable.
  protected ArrayList<String> hivePartitionColumns = new ArrayList<String>();
  protected ArrayList<String> partition;
  private transient String localString = "";

  /**
   * The file system used to write to.
   */
  protected transient FileSystem fs;
  @Nonnull
  protected String tablename;
  @Nonnull
  protected String hivepath;

  @Override
  public void setup(OperatorContext context)
  {
    try {
      fs = getHDFSInstance();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
     super.setup(context);
  }

  /**
   * Override this method to change the FileSystem instance that is used by the operator.
   *
   * @return A FileSystem object.
   * @throws IOException
   */
  protected FileSystem getHDFSInstance() throws IOException
  {
    FileSystem tempFS = FileSystem.newInstance(new Path(store.filepath).toUri(), new Configuration());
    if (!tempFS.getScheme().equalsIgnoreCase("hdfs")) {
      localString = " local";
    }
    return tempFS;
  }

  /**
   * Function to process each incoming tuple
   * This can be overridden by user for multiple partition columns.
   * Giving an implementation for one partition column.
   *
   * @param tuple incoming tuple which has filename and hive partition.
   */
  @Override
  public void processTuple(FilePartitionMapping tuple)
  {
    String command = processHiveFile(tuple);
    if (command != null) {
      Statement stmt;
      try {
        stmt = store.getConnection().createStatement();
        stmt.execute(command);
      }
      catch (SQLException ex) {
        throw new RuntimeException("Moving file into hive failed" + ex);
      }
    }

  }

  public String processHiveFile(FilePartitionMapping tuple)
  {
    String fileMoved = tuple.getFilename();
    partition = tuple.getPartition();
    String command = getInsertCommand(fileMoved);
    return command;
  }

  /*
   * User can specify multiple partitions here, giving a default implementation for one partition column here.
   */
  protected String getInsertCommand(String filepath)
  {
    String command = null;
    filepath = store.getFilepath() + Path.SEPARATOR + filepath;
    logger.debug("processing {} filepath", filepath);
    int numPartitions = partition.size();
    try {
      if (fs.exists(new Path(filepath))) {
        if (numPartitions > 0) {
          StringBuilder partitionString = new StringBuilder(getHivePartitionColumns().get(0) + "='" + partition.get(0) + "'");
          int i = 0;
          while (i < numPartitions) {
            i++;
            if (i == numPartitions) {
              break;
            }
            partitionString.append(",").append(getHivePartitionColumns().get(i)).append("='").append(partition.get(i)).append("'");
          }
          if (i < hivePartitionColumns.size()) {
            partitionString.append(",").append(getHivePartitionColumns().get(i));
          }
          command = "load data" + localString + " inpath '" + filepath + "' into table " + tablename + " PARTITION" + "( " + partitionString + " )";
        }
        else {
          command = "load data" + localString + " inpath '" + filepath + "' into table " + tablename;
        }
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    logger.debug("command is {}" , command);
    return command;
  }

  public ArrayList<String> getHivePartitionColumns()
  {
    return hivePartitionColumns;
  }

  public void setHivePartitionColumns(ArrayList<String> hivePartitionColumns)
  {
    this.hivePartitionColumns = hivePartitionColumns;
  }

  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  private static final Logger logger = LoggerFactory.getLogger(HiveOperator.class);
}
