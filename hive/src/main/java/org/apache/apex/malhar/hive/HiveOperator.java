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

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.hive.AbstractFSRollingOutputOperator.FilePartitionMapping;
import org.apache.apex.malhar.lib.counters.BasicCounters;
import org.apache.apex.malhar.lib.db.AbstractStoreOutputOperator;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * Hive operator which can insert data in txt format in tables/partitions from a
 * file written in hdfs location. The file contains data of the same data type
 * as the hive tables created by user and is already committed. No changes will
 * be made to the input file once its given to HiveOperator. This is a fault
 * tolerant implementation of HiveOperator which assumes that load operation is
 * an atomic operation in Hive.
 *
 * @category Output
 * @tags database, sql, hive
 *
 * @since 2.1.0
 */
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class HiveOperator extends AbstractStoreOutputOperator<FilePartitionMapping, HiveStore>
{
  //This Property is user configurable.
  protected ArrayList<String> hivePartitionColumns = new ArrayList<String>();
  private transient String localString = "";

  /**
   * Hive store.
   *
   * @deprecated use {@link AbstractStoreOutputOperator#store} instead
   */
  @Deprecated
  protected HiveStore hivestore;

  /**
   * The file system used to write to.
   */
  protected transient FileSystem fs;
  @Nonnull
  protected String tablename;
  @Nonnull
  protected String hivepath;
  /**
   * This is the operator context passed at setup.
   */
  private transient OperatorContext context;

  /**
   * File output counters.
   */
  private final BasicCounters<MutableLong> fileCounters = new BasicCounters<MutableLong>(MutableLong.class);

  /**
   * The total time in milliseconds the operator has been running for.
   */
  private long totalTime;
  /**
   * Last time stamp collected.
   */
  private long lastTimeStamp;
  /**
   * The total number of bytes written by the operator.
   */
  protected long totalBytesWritten = 0;

  public HiveOperator()
  {
    store = new HiveStore();
    hivestore = store;
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      fs = getHDFSInstance();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    this.context = context;
    lastTimeStamp = System.currentTimeMillis();

    fileCounters.setCounter(Counters.TOTAL_BYTES_WRITTEN, new MutableLong());
    fileCounters.setCounter(Counters.TOTAL_TIME_ELAPSED, new MutableLong());
    super.setup(context);
  }

  /**
   * Override this method to change the FileSystem instance that is used by the
   * operator.
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
   * Function to process each incoming tuple. The input is FilePartitionMapping
   * which is a POJO containing filename which is already committed and will not
   * be changed.The POJO also contains the hive partitions to which the
   * respective files will be moved.
   *
   * @param tuple
   *          incoming tuple which has filename and hive partition.
   */
  @Override
  public void processTuple(FilePartitionMapping tuple)
  {
    String command = processHiveFile(tuple);
    logger.debug("commands is {}", command);
    if (command != null) {
      Statement stmt;
      try {
        stmt = store.getConnection().createStatement();
        stmt.execute(command);
      } catch (SQLException ex) {
        throw new RuntimeException("Moving file into hive failed" + ex);
      }
    }

  }

  /*
   * This function extracts the filename and partitions to which the file will be loaded.
   * It returns the command to be executed by hive.
   */
  private String processHiveFile(FilePartitionMapping tuple)
  {
    String filename = tuple.getFilename();
    ArrayList<String> partition = tuple.getPartition();
    String command = null;
    String filepath = store.getFilepath() + Path.SEPARATOR + filename;
    logger.debug("processing {} filepath", filepath);
    int numPartitions = partition.size();
    try {
      if (fs.exists(new Path(filepath))) {
        if (numPartitions > 0) {
          StringBuilder partitionString = new StringBuilder(
              hivePartitionColumns.get(0) + "='" + partition.get(0) + "'");
          int i = 0;
          while (i < numPartitions) {
            i++;
            if (i == numPartitions) {
              break;
            }
            partitionString.append(",").append(hivePartitionColumns.get(i)).append("='").append(partition.get(i))
                .append("'");
          }
          if (i < hivePartitionColumns.size()) {
            partitionString.append(",").append(hivePartitionColumns.get(i));
          }
          command = "load data" + localString + " inpath '" + filepath + "' into table " + tablename + " PARTITION"
              + "( " + partitionString + " )";
        } else {
          command = "load data" + localString + " inpath '" + filepath + "' into table " + tablename;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    logger.debug("command is {}", command);
    return command;
  }

  @Override
  public void endWindow()
  {
    long currentTimeStamp = System.currentTimeMillis();
    totalTime += currentTimeStamp - lastTimeStamp;
    lastTimeStamp = currentTimeStamp;

    fileCounters.getCounter(Counters.TOTAL_TIME_ELAPSED).setValue(totalTime);
    fileCounters.getCounter(Counters.TOTAL_BYTES_WRITTEN).setValue(totalBytesWritten);
    context.setCounters(fileCounters);
  }

  public void teardown()
  {
    long currentTimeStamp = System.currentTimeMillis();
    totalTime += currentTimeStamp - lastTimeStamp;
    lastTimeStamp = currentTimeStamp;
  }

  /**
   * Get the partition columns in hive to which data needs to be loaded.
   *
   * @return List of Hive Partition Columns
   */
  public ArrayList<String> getHivePartitionColumns()
  {
    return hivePartitionColumns;
  }

  /**
   * Set the hive partition columns to which data needs to be loaded.
   *
   * @param hivePartitionColumns
   */
  public void setHivePartitionColumns(ArrayList<String> hivePartitionColumns)
  {
    this.hivePartitionColumns = hivePartitionColumns;
  }

  /**
   * Get the table name in hive.
   *
   * @return table name
   */
  public String getTablename()
  {
    return tablename;
  }

  /**
   * Set the table name in hive.
   *
   * @param tablename
   */
  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  /**
   * Gets the store set for hive;
   *
   * @deprecated use {@link #getStore()} instead.
   * @return hive store
   */
  @Deprecated
  public HiveStore getHivestore()
  {
    return store;
  }

  /**
   * Set the store in hive.
   *
   * @deprecated use {@link #setStore()} instead.
   * @param hivestore
   */
  @Deprecated
  public void setHivestore(HiveStore hivestore)
  {
    this.hivestore = hivestore;
    super.setStore(hivestore);
  }

  public static enum Counters
  {
    /**
     * An enum for counters representing the total number of bytes written by
     * the operator.
     */
    TOTAL_BYTES_WRITTEN,

    /**
     * An enum for counters representing the total time the operator has been
     * operational for.
     */
    TOTAL_TIME_ELAPSED
  }

  private static final Logger logger = LoggerFactory.getLogger(HiveOperator.class);
}
