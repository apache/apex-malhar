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
package com.datatorrent.lib.db.jdbc;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.Min;

import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.Operator.IdleTimeHandler;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.lib.db.AbstractStoreInputOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.KryoCloneUtils;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Abstract operator for for consuming data using JDBC interface<br>
 * User needs User needs to provide
 * tableName,dbConnection,setEmitColumnList,look-up key <br>
 * Optionally batchSize,pollInterval,Look-up key and a where clause can be given
 * <br>
 * This operator uses static partitioning to arrive at range queries for exactly
 * once reads<br>
 * This operator will create a configured number of non-polling static
 * partitions for fetching the existing data in the table. And an additional
 * single partition for polling additive data. Assumption is that there is an
 * ordered column using which range queries can be formed<br>
 * If an emitColumnList is provided, please ensure that the keyColumn is the
 * first column in the list<br>
 * Range queries are formed using the {@link JdbcMetaDataUtility}} Output -
 * comma separated list of the emit columns eg columnA,columnB,columnC<br>
 * Only newly added data which has increasing ids will be fetched by the polling
 * jdbc partition
 * 
 * In the next iterations this operator would support an in-clause for
 * idempotency instead of having only range query support to support non ordered
 * key columns
 * 
 * 
 * @displayName Jdbc Polling Input Operator
 * @category Input
 * @tags database, sql, jdbc, partitionable,exactlyOnce
 */
@Evolving
public abstract class AbstractJdbcPollInputOperator<T> extends AbstractStoreInputOperator<T, JdbcStore>
    implements ActivationListener<Context>, IdleTimeHandler, Partitioner<AbstractJdbcPollInputOperator<T>>
{
  /**
   * poll interval in milliseconds
   */
  private static int pollInterval = 10000;

  @Min(1)
  private int partitionCount = 1;
  protected transient int operatorId;
  protected transient boolean isReplayed;
  protected transient boolean isPollable;
  protected int batchSize;
  protected static int fetchSize = 20000;
  /**
   * Map of windowId to <lower bound,upper bound> of the range key
   */
  protected transient MutablePair<String, String> currentWindowRecoveryState;

  /**
   * size of the emit queue used to hold polled records before emit
   */
  private static int queueCapacity = 4 * 1024 * 1024;
  private transient volatile boolean execute;
  private transient AtomicReference<Throwable> cause;
  protected transient int spinMillis;
  private transient OperatorContext context;
  protected String tableName;
  protected String key;
  protected long currentWindowId;
  protected KeyValPair<String, String> rangeQueryPair;
  protected String lower;
  protected String upper;
  protected boolean recovered;
  protected boolean isPolled;
  protected String whereCondition = null;
  protected String previousUpperBound;
  protected String highestPolled;
  private static final String user = "";
  private static final String password = "";
  /**
   * thread to poll database
   */
  private transient Thread dbPoller;
  protected transient ArrayBlockingQueue<List<T>> emitQueue;
  protected transient PreparedStatement ps;
  protected WindowDataManager windowManager;
  private String emitColumnList;

  /**
   * Returns the where clause
   */
  public String getWhereCondition()
  {
    return whereCondition;
  }

  /**
   * Sets the where clause
   */
  public void setWhereCondition(String whereCondition)
  {
    this.whereCondition = whereCondition;
  }

  /**
   * Returns the list of columns to select from the query
   */
  public String getEmitColumnList()
  {
    return emitColumnList;
  }

  /**
   * Comma separated list of columns to select from the given table
   */
  public void setEmitColumnList(String emitColumnList)
  {
    this.emitColumnList = emitColumnList;
  }

  /**
   * Returns the fetchsize for getting the results
   */
  public int getFetchSize()
  {
    return fetchSize;
  }

  /**
   * Sets the fetchsize for getting the results
   */
  public void setFetchSize(int fetchSize)
  {
    this.fetchSize = fetchSize;
  }

  protected abstract void pollRecords(PreparedStatement ps);

  /**
   * Returns the interval for polling the queue
   */
  public int getPollInterval()
  {
    return pollInterval;
  }

  /**
   * Sets the interval for polling the emit queue
   */
  public void setPollInterval(int pollInterval)
  {
    this.pollInterval = pollInterval;
  }

  /**
   * Returns the capacity of the emit queue
   */
  public int getQueueCapacity()
  {
    return queueCapacity;
  }

  /**
   * Sets the capacity of the emit queue
   */
  public void setQueueCapacity(int queueCapacity)
  {
    this.queueCapacity = queueCapacity;
  }

  /**
   * Returns the ordered key used to generate the range queries
   */
  public String getKey()
  {
    return key;
  }

  /**
   * Sets the ordered key used to generate the range queries
   */
  public void setKey(String key)
  {
    this.key = key;
  }

  /**
   * Returns the tableName which would be queried
   */
  public String getTableName()
  {
    return tableName;
  }

  /**
   * Sets the tableName to query
   */
  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  /**
   * Returns rangeQueryPair - <lowerBound,upperBound>
   */
  public KeyValPair<String, String> getRangeQueryPair()
  {
    return rangeQueryPair;
  }

  /**
   * Sets the rangeQueryPair <lowerBound,upperBound>
   */
  public void setRangeQueryPair(KeyValPair<String, String> rangeQueryPair)
  {
    this.rangeQueryPair = rangeQueryPair;
  }

  /**
   * Returns batchSize indicating the number of elements in emitQueue
   */
  public int getBatchSize()
  {
    return batchSize;
  }

  /**
   * Sets batchSize for number of elements in the emitQueue
   */
  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public AbstractJdbcPollInputOperator()
  {
    currentWindowRecoveryState = new MutablePair<>();
    windowManager = new FSWindowDataManager();
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    spinMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
    execute = true;
    cause = new AtomicReference<Throwable>();
    emitQueue = new ArrayBlockingQueue<List<T>>(queueCapacity);
    this.context = context;
    operatorId = context.getId();

    try {

      //If its a range query pass upper and lower bounds
      //If its a polling query pass only the lower bound
      if (getRangeQueryPair().getValue() != null) {
        ps = store.getConnection()
            .prepareStatement(
                JdbcMetaDataUtility.buildRangeQuery(getTableName(), getKey(), rangeQueryPair.getKey(),
                    rangeQueryPair.getValue()),
                java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
      } else {
        ps = store.getConnection().prepareStatement(
            JdbcMetaDataUtility.buildPollableQuery(getTableName(), getKey(), rangeQueryPair.getKey()),
            java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        isPollable = true;
      }

    } catch (SQLException e) {
      LOG.error("Exception in initializing the range query for a given partition", e);
      throw new RuntimeException(e);
    }

    windowManager.setup(context);
    LOG.debug("super setup done...");
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;

    isReplayed = false;

    if (currentWindowId <= windowManager.getLargestRecoveryWindow()) {
      try {
        replay(currentWindowId);
      } catch (SQLException e) {
        LOG.error("Exception in replayed windows", e);
        throw new RuntimeException(e);
      }
    }

    if (isReplayed && currentWindowId == windowManager.getLargestRecoveryWindow()) {
      try {
        if (!isPollable && rangeQueryPair.getValue() != null) {

          ps = store.getConnection().prepareStatement(
              JdbcMetaDataUtility.buildGTRangeQuery(getTableName(), getKey(), previousUpperBound,
                  rangeQueryPair.getValue()),
              java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        } else {
          String bound = null;
          if (previousUpperBound == null) {
            bound = getRangeQueryPair().getKey();
          } else {
            bound = previousUpperBound;
          }
          ps = store.getConnection().prepareStatement(
              JdbcMetaDataUtility.buildPollableQuery(getTableName(), getKey(), bound),
              java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
          isPollable = true;
        }
        isReplayed = false;
        LOG.debug("Prepared statement after re-initialization - {} ", ps.toString());
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        throw new RuntimeException(e);
      }
    }

    //Reset the pollable query with the updated upper and lower bounds
    if (isPollable) {
      try {
        String bound = null;
        if (previousUpperBound == null && highestPolled == null) {
          bound = getRangeQueryPair().getKey();
        } else {
          bound = highestPolled;
        }
        ps = store.getConnection().prepareStatement(
            JdbcMetaDataUtility.buildPollableQuery(getTableName(), getKey(), bound),
            java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        LOG.debug("Polling query {} {}", ps.toString(), currentWindowId);
        isPolled = false;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    lower = null;
    upper = null;

    //Check if a thread is already active and start only if its no
    //Do not start the thread from setup, will conflict with the replay
    if (dbPoller == null && !isReplayed) {
      //If this is not a replayed state, reset the ps to highest read offset + 1, 
      //keep the upper bound as the one that was initialized after static partitioning
      LOG.info("Statement when re-initialized {}", ps.toString());
      dbPoller = new Thread(new DBPoller());
      dbPoller.start();
    }
  }

  @Override
  public void emitTuples()
  {
    if (isReplayed) {
      return;
    }

    List<T> tuples;

    if ((tuples = emitQueue.poll()) != null) {
      for (Object tuple : tuples) {
        if (lower == null) {
          lower = tuple.toString();
        }
        upper = tuple.toString();
        outputPort.emit((T)tuple);
      }
    }
  }

  @Override
  public void endWindow()
  {
    try {
      if (currentWindowId > windowManager.getLargestRecoveryWindow()) {
        if (currentWindowRecoveryState == null) {
          currentWindowRecoveryState = new MutablePair<String, String>();
        }
        if (lower != null && upper != null) {
          previousUpperBound = upper;
          isPolled = true;
        }
        MutablePair<String, String> windowBoundaryPair = new MutablePair<>(lower, upper);
        currentWindowRecoveryState = windowBoundaryPair;
        windowManager.save(currentWindowRecoveryState, currentWindowId);
      }
    } catch (IOException e) {
      throw new RuntimeException("saving recovery", e);
    }
    currentWindowRecoveryState = new MutablePair<>();
  }

  public int getPartitionCount()
  {
    return partitionCount;
  }

  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  @Override
  public void activate(Context cntxt)
  {
    if (context.getValue(OperatorContext.ACTIVATION_WINDOW_ID) != Stateless.WINDOW_ID
        && context.getValue(OperatorContext.ACTIVATION_WINDOW_ID) < windowManager.getLargestRecoveryWindow()) {
      // If it is a replay state, don't start any threads here
      return;
    }
  }

  @Override
  public void deactivate()
  {
    try {
      if (dbPoller != null && dbPoller.isAlive()) {
        dbPoller.interrupt();
        dbPoller.join();
      }
    } catch (InterruptedException ex) {
      // log and ignore, ending execution anyway
      LOG.error("exception in poller thread: ", ex);
    }
  }

  @Override
  public void handleIdleTime()
  {
    if (execute) {
      try {
        Thread.sleep(spinMillis);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    } else {
      LOG.error("Exception: ", cause);
      DTThrowable.rethrow(cause.get());
    }
  }

  protected void replay(long windowId) throws SQLException
  {
    isReplayed = true;

    MutablePair<String, String> recoveredData = new MutablePair<String, String>();
    try {
      recoveredData = (MutablePair<String, String>)windowManager.retrieve(windowId);

      if (recoveredData != null) {
        //skip the window and return if there was no incoming data in the window
        if (recoveredData.left == null || recoveredData.right == null) {
          return;
        }

        if (recoveredData.right.equals(rangeQueryPair.getValue()) || recoveredData.right.equals(previousUpperBound)) {
          LOG.info("Matched so returning");
          return;
        }

        JdbcPollInputOperator jdbcPoller = new JdbcPollInputOperator();
        jdbcPoller.setStore(store);
        jdbcPoller.setKey(getKey());
        jdbcPoller.setPartitionCount(getPartitionCount());
        jdbcPoller.setPollInterval(getPollInterval());
        jdbcPoller.setTableName(getTableName());
        jdbcPoller.setBatchSize(getBatchSize());
        isPollable = false;

        LOG.debug("[Window ID -" + windowId + "," + recoveredData.left + "," + recoveredData.right + "]");

        jdbcPoller.setRangeQueryPair(new KeyValPair<String, String>(recoveredData.left, recoveredData.right));

        jdbcPoller.ps = jdbcPoller.store.getConnection().prepareStatement(
            JdbcMetaDataUtility.buildRangeQuery(jdbcPoller.getTableName(), jdbcPoller.getKey(),
                jdbcPoller.getRangeQueryPair().getKey(), jdbcPoller.getRangeQueryPair().getValue()),
            java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        LOG.info("Query formed for recovered data - {}", jdbcPoller.ps.toString());

        emitReplayedTuples(jdbcPoller.ps);
      }
    } catch (IOException e) {
      throw new RuntimeException("replay", e);
    }

  }

  /**
   * Replays the tuples in sync mode for replayed windows
   */
  public void emitReplayedTuples(PreparedStatement ps)
  {
    LOG.debug("Emitting replayed statement is -" + ps.toString());
    ResultSet rs = null;
    try (PreparedStatement pStat = ps;) {
      pStat.setFetchSize(getFetchSize());
      LOG.debug("sql query = {}", pStat);
      rs = pStat.executeQuery();
      if (rs == null || rs.isClosed()) {
        LOG.debug("Nothing to replay");
        return;
      }
      while (rs.next()) {
        previousUpperBound = rs.getObject(getKey()).toString();
        outputPort.emit((T)rs.getObject(getKey()));
      }
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Uses a static partitioning scheme to initialize operator partitions with
   * non-overlapping key ranges to read In addition to 'n' partitions, 'n+1'
   * partition is a polling partition which reads the records beyond the given
   * range
   */
  @Override
  public Collection<com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<T>>> definePartitions(
      Collection<com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<T>>> partitions,
      com.datatorrent.api.Partitioner.PartitioningContext context)
  {
    List<Partition<AbstractJdbcPollInputOperator<T>>> newPartitions = new ArrayList<Partition<AbstractJdbcPollInputOperator<T>>>(
        getPartitionCount());
    JdbcStore jdbcStore = new JdbcStore();
    jdbcStore.setDatabaseDriver(store.getDatabaseDriver());
    jdbcStore.setDatabaseUrl(store.getDatabaseUrl());
    jdbcStore.setConnectionProperties(store.getConnectionProperties());

    jdbcStore.connect();

    HashMap<Integer, KeyValPair<String, String>> partitionToRangeMap = null;
    try {
      partitionToRangeMap = JdbcMetaDataUtility.getPartitionedQueryMap(getPartitionCount(),
          jdbcStore.getDatabaseDriver(), jdbcStore.getDatabaseUrl(), getTableName(), getKey(),
          store.getConnectionProperties().getProperty(user), store.getConnectionProperties().getProperty(password),
          whereCondition, emitColumnList);
    } catch (SQLException e) {
      LOG.error("Exception in initializing the partition range", e);
    }

    KryoCloneUtils<AbstractJdbcPollInputOperator<T>> cloneUtils = KryoCloneUtils.createCloneUtils(this);

    for (int i = 0; i <= getPartitionCount(); i++) {
      AbstractJdbcPollInputOperator<T> jdbcPoller = null;

      jdbcPoller = cloneUtils.getClone();

      jdbcPoller.setStore(store);
      jdbcPoller.setKey(getKey());
      jdbcPoller.setPartitionCount(getPartitionCount());
      jdbcPoller.setPollInterval(getPollInterval());
      jdbcPoller.setTableName(getTableName());
      jdbcPoller.setBatchSize(getBatchSize());
      jdbcPoller.setEmitColumnList(getEmitColumnList());

      store.connect();
      //The n given partitions are for range queries and n + 1 partition is for polling query
      //The upper bound for the n+1 partition is set to null since its a pollable partition
      if (i < getPartitionCount()) {
        jdbcPoller.setRangeQueryPair(partitionToRangeMap.get(i));
        isPollable = false;
      } else {
        jdbcPoller.setRangeQueryPair(new KeyValPair<String, String>(partitionToRangeMap.get(i - 1).getValue(), null));
        isPollable = true;
      }
      Partition<AbstractJdbcPollInputOperator<T>> po = new DefaultPartition<AbstractJdbcPollInputOperator<T>>(
          jdbcPoller);
      newPartitions.add(po);
    }

    previousUpperBound = null;
    return newPartitions;
  }

  @Override
  public void partitioned(
      Map<Integer, com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<T>>> partitions)
  {
    //Nothing to implement here
  }

  /**
   * This class polls a store that can be queried with a JDBC interface The
   * preparedStatement is updated as more rows are read
   */
  public class DBPoller implements Runnable
  {
    @Override
    public void run()
    {
      while (execute) {
        try {
          long startTs = System.currentTimeMillis();
          if ((isPollable && !isPolled) || !isPollable) {
            pollRecords(ps);
          }
          long endTs = System.currentTimeMillis();
          long ioTime = endTs - startTs;
          long sleepTime = pollInterval - ioTime;
          LOG.debug("pollInterval = {} , I/O time = {} , sleepTime = {}", pollInterval, ioTime, sleepTime);
          Thread.sleep(sleepTime > 0 ? sleepTime : 0);
        } catch (Exception ex) {
          cause.set(ex);
          execute = false;
        }
      }
    }
  }

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AbstractJdbcPollInputOperator.class);

}
