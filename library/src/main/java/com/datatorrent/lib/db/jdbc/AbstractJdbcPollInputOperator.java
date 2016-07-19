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
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SelectField;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.annotation.OperatorAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.lib.db.AbstractStoreInputOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.KryoCloneUtils;
import com.datatorrent.netlet.util.DTThrowable;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.jooq.impl.DSL.field;

/**
 * Abstract operator for consuming data using JDBC interface<br>
 * User needs to provide tableName, dbConnection, columnsExpression, look-up key<br>
 * Optionally batchSize, pollInterval and a where clause can be given <br>
 * This operator uses static partitioning to arrive at range queries to
 * idempotent reads<br>
 * This operator will create a configured number of non-polling static
 * partitions for fetching the existing data in the table. And an additional
 * single partition for polling additive data. Assumption is that there is an
 * ordered unique column using which range queries can be formed<br>
 * 
 * Only newly added data will be fetched by the polling jdbc partition, also
 * assumption is rows won't be added or deleted in middle during scan.
 * 
 * 
 * @displayName Jdbc Polling Input Operator
 * @category Input
 * @tags database, sql, jdbc, partitionable, idepotent, pollable
 */
@Evolving
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class AbstractJdbcPollInputOperator<T> extends AbstractStoreInputOperator<T, JdbcStore> implements
    ActivationListener<OperatorContext>, Partitioner<AbstractJdbcPollInputOperator<T>>
{
  private static int DEFAULT_QUEUE_CAPACITY = 4 * 1024;
  private static int DEFAULT_POLL_INTERVAL = 10 * 1000;
  private static int DEFAULT_FETCH_SIZE = 20000;
  private static int DEFAULT_BATCH_SIZE = 2000;
  private static int DEFAULT_SLEEP_TIME = 100;
  private int pollInterval = DEFAULT_POLL_INTERVAL; //in miliseconds
  private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
  private int fetchSize = DEFAULT_FETCH_SIZE;

  @Min(1)
  private int partitionCount = 1;
  private int batchSize = DEFAULT_BATCH_SIZE;

  @NotNull
  private String tableName;
  @NotNull
  private String columnsExpression;
  @NotNull
  private String key;
  private String whereCondition = null;
  private long currentWindowId;
  private WindowDataManager windowManager;

  protected KeyValPair<Integer, Integer> rangeQueryPair;
  protected Integer lowerBound;
  protected Integer lastEmittedRow;
  private transient int operatorId;
  private transient DSLContext create;
  private transient volatile boolean execute;
  private transient ScheduledExecutorService scanService;
  private transient AtomicReference<Throwable> threadException;
  protected transient boolean isPolled;
  protected transient Integer lastPolledRow;
  protected transient LinkedBlockingDeque<T> emitQueue;
  protected transient PreparedStatement ps;
  protected boolean isPollerPartition;

  protected transient MutablePair<Integer, Integer> currentWindowRecoveryState;

  public AbstractJdbcPollInputOperator()
  {
    currentWindowRecoveryState = new MutablePair<>();
    windowManager = new FSWindowDataManager();
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    intializeDSLContext();
    if (scanService == null) {
      scanService = Executors.newScheduledThreadPool(1);
    }
    execute = true;
    emitQueue = new LinkedBlockingDeque<>(queueCapacity);
    operatorId = context.getId();
    windowManager.setup(context);
  }

  private void intializeDSLContext()
  {
    create = DSL.using(store.getConnection(), JDBCUtils.dialect(store.getDatabaseUrl()));
  }

  @Override
  public void activate(OperatorContext context)
  {
    initializePreparedStatement();
    long largestRecoveryWindow = windowManager.getLargestRecoveryWindow();
    if (largestRecoveryWindow == Stateless.WINDOW_ID
        || context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) > largestRecoveryWindow) {
      scanService.scheduleAtFixedRate(new DBPoller(), 0, pollInterval, TimeUnit.MILLISECONDS);
    }
  }

  protected void initializePreparedStatement()
  {
    try {
      // If its a range query pass upper and lower bounds, If its a polling query pass only the lower bound
      if (isPollerPartition) {
        ps = store.getConnection().prepareStatement(buildRangeQuery(rangeQueryPair.getKey(), Integer.MAX_VALUE),
            TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
      } else {
        ps = store.getConnection().prepareStatement(
            buildRangeQuery(rangeQueryPair.getKey(), (rangeQueryPair.getValue() - rangeQueryPair.getKey())),
            TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
      }
    } catch (SQLException e) {
      LOG.error("Exception in initializing the range query for a given partition", e);
      throw new RuntimeException(e);
    }

  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    if (currentWindowId <= windowManager.getLargestRecoveryWindow()) {
      try {
        replay(currentWindowId);
        return;
      } catch (SQLException e) {
        LOG.error("Exception in replayed windows", e);
        throw new RuntimeException(e);
      }
    }
    if (isPollerPartition) {
      updatePollQuery();
      isPolled = false;
    }
    lowerBound = lastEmittedRow;
  }

  private void updatePollQuery()
  {
    if ((lastPolledRow != lastEmittedRow)) {
      if (lastEmittedRow == null) {
        lastPolledRow = rangeQueryPair.getKey();
      } else {
        lastPolledRow = lastEmittedRow;
      }
      try {
        ps = store.getConnection().prepareStatement(buildRangeQuery(lastPolledRow, Integer.MAX_VALUE),
            TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

  }

  @Override
  public void emitTuples()
  {
    if (currentWindowId <= windowManager.getLargestRecoveryWindow()) {
      return;
    }
    int pollSize = (emitQueue.size() < batchSize) ? emitQueue.size() : batchSize;
    while (pollSize-- > 0) {
      T obj = emitQueue.poll();
      if (obj != null) {
        emitTuple(obj);
      }
      lastEmittedRow++;
    }
  }

  protected abstract void emitTuple(T tuple);

  @Override
  public void endWindow()
  {
    try {
      if (currentWindowId > windowManager.getLargestRecoveryWindow()) {
        currentWindowRecoveryState = new MutablePair<>(lowerBound, lastEmittedRow);
        windowManager.save(currentWindowRecoveryState, operatorId, currentWindowId);
      }
    } catch (IOException e) {
      throw new RuntimeException("saving recovery", e);
    }
    if (threadException != null) {
      store.disconnect();
      DTThrowable.rethrow(threadException.get());
    }
  }

  @Override
  public void deactivate()
  {
    scanService.shutdownNow();
    store.disconnect();
  }

  protected void pollRecords()
  {
    if (isPolled) {
      return;
    }
    try {
      ps.setFetchSize(getFetchSize());
      ResultSet result = ps.executeQuery();
      if (result.next()) {
        do {
          while (!emitQueue.offer(getTuple(result))) {
            Thread.sleep(DEFAULT_SLEEP_TIME);
          }
        } while (result.next());
      }
      isPolled = true;
    } catch (SQLException ex) {
      execute = false;
      threadException = new AtomicReference<Throwable>(ex);
    } catch (InterruptedException e) {
      threadException = new AtomicReference<Throwable>(e);
    } finally {
      if (!isPollerPartition) {
        store.disconnect();
      }
    }
  }

  public abstract T getTuple(ResultSet result);

  protected void replay(long windowId) throws SQLException
  {

    try {
      MutablePair<Integer, Integer> recoveredData = (MutablePair<Integer, Integer>)windowManager.load(operatorId,
          windowId);

      if (recoveredData != null && shouldReplayWindow(recoveredData)) {
        LOG.debug("[Recovering Window ID - {} for record range: {}, {}]", windowId, recoveredData.left,
            recoveredData.right);

        ps = store.getConnection().prepareStatement(
            buildRangeQuery(recoveredData.left, (recoveredData.right - recoveredData.left)), TYPE_FORWARD_ONLY,
            CONCUR_READ_ONLY);
        LOG.info("Query formed to recover data - {}", ps.toString());

        emitReplayedTuples(ps);

      }

      if (currentWindowId == windowManager.getLargestRecoveryWindow()) {
        try {
          if (!isPollerPartition && rangeQueryPair.getValue() != null) {
            ps = store.getConnection().prepareStatement(
                buildRangeQuery(lastEmittedRow, (rangeQueryPair.getValue() - lastEmittedRow)), TYPE_FORWARD_ONLY,
                CONCUR_READ_ONLY);
          } else {
            Integer bound = null;
            if (lastEmittedRow == null) {
              bound = rangeQueryPair.getKey();
            } else {
              bound = lastEmittedRow;
            }
            ps = store.getConnection().prepareStatement(buildRangeQuery(bound, Integer.MAX_VALUE), TYPE_FORWARD_ONLY,
                CONCUR_READ_ONLY);
          }
          scanService.scheduleAtFixedRate(new DBPoller(), 0, pollInterval, TimeUnit.MILLISECONDS);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Exception during replay of records.", e);
    }

  }

  private boolean shouldReplayWindow(MutablePair<Integer, Integer> recoveredData)
  {
    if (recoveredData.left == null || recoveredData.right == null) {
      return false;
    }
    if (recoveredData.right.equals(rangeQueryPair.getValue()) || recoveredData.right.equals(lastEmittedRow)) {
      return false;
    }
    return true;
  }

  /**
   * Replays the tuples in sync mode for replayed windows
   */
  public void emitReplayedTuples(PreparedStatement ps)
  {
    ResultSet rs = null;
    try (PreparedStatement pStat = ps;) {
      pStat.setFetchSize(getFetchSize());
      rs = pStat.executeQuery();
      if (rs == null || rs.isClosed()) {
        return;
      }
      while (rs.next()) {
        emitTuple(getTuple(rs));
        lastEmittedRow++;
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
      Collection<Partition<AbstractJdbcPollInputOperator<T>>> partitions, PartitioningContext context)
  {
    List<Partition<AbstractJdbcPollInputOperator<T>>> newPartitions = new ArrayList<Partition<AbstractJdbcPollInputOperator<T>>>(
        getPartitionCount());

    HashMap<Integer, KeyValPair<Integer, Integer>> partitionToRangeMap = null;
    try {
      store.connect();
      intializeDSLContext();
      partitionToRangeMap = getPartitionedQueryRangeMap(getPartitionCount());
    } catch (SQLException e) {
      LOG.error("Exception in initializing the partition range", e);
      throw new RuntimeException(e);
    } finally {
      store.disconnect();
    }

    KryoCloneUtils<AbstractJdbcPollInputOperator<T>> cloneUtils = KryoCloneUtils.createCloneUtils(this);

    // The n given partitions are for range queries and n + 1 partition is for polling query
    for (int i = 0; i <= getPartitionCount(); i++) {
      AbstractJdbcPollInputOperator<T> jdbcPoller = cloneUtils.getClone();
      if (i < getPartitionCount()) {
        jdbcPoller.rangeQueryPair = partitionToRangeMap.get(i);
        jdbcPoller.lastEmittedRow = partitionToRangeMap.get(i).getKey();
        jdbcPoller.isPollerPartition = false;
      } else {
        // The upper bound for the n+1 partition is set to null since its a pollable partition
        int partitionKey = partitionToRangeMap.get(i - 1).getValue();
        jdbcPoller.rangeQueryPair = new KeyValPair<Integer, Integer>(partitionKey, null);
        jdbcPoller.lastEmittedRow = partitionKey;
        jdbcPoller.isPollerPartition = true;
      }
      newPartitions.add(new DefaultPartition<AbstractJdbcPollInputOperator<T>>(jdbcPoller));
    }

    return newPartitions;
  }

  @Override
  public void partitioned(
      Map<Integer, com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<T>>> partitions)
  {
    // Nothing to implement here
  }

  private HashMap<Integer, KeyValPair<Integer, Integer>> getPartitionedQueryRangeMap(int partitions)
      throws SQLException
  {
    int rowCount = 0;
    try {
      rowCount = getRecordsCount();
    } catch (SQLException e) {
      LOG.error("Exception in getting the record range", e);
    }

    HashMap<Integer, KeyValPair<Integer, Integer>> partitionToQueryMap = new HashMap<>();
    int events = (rowCount / partitions);
    for (int i = 0, lowerOffset = 0, upperOffset = events; i < partitions - 1; i++, lowerOffset += events, upperOffset += events) {
      partitionToQueryMap.put(i, new KeyValPair<Integer, Integer>(lowerOffset, upperOffset));
    }

    partitionToQueryMap.put(partitions - 1, new KeyValPair<Integer, Integer>(events * (partitions - 1), (int)rowCount));
    LOG.info("Partition map - " + partitionToQueryMap.toString());
    return partitionToQueryMap;
  }

  /**
   * Finds the total number of rows in the table
   *
   * @return number of records in table
   */
  private int getRecordsCount() throws SQLException
  {
    Condition condition = DSL.trueCondition();
    if (getWhereCondition() != null) {
      condition = condition.and(getWhereCondition());
    }
    int recordsCount = create.select(DSL.count()).from(getTableName()).where(condition).fetchOne(0, int.class);
    return recordsCount;
  }

  /**
   * Helper function returns a range query based on the bounds passed<br>
   */
  protected String buildRangeQuery(int offset, int limit)
  {
    Condition condition = DSL.trueCondition();
    if (getWhereCondition() != null) {
      condition = condition.and(getWhereCondition());
    }

    String sqlQuery;
    if (getColumnsExpression() != null) {
      Collection<Field<?>> columns = new ArrayList<>();
      for (String column : getColumnsExpression().split(",")) {
        columns.add(field(column));
      }
      sqlQuery = create.select((Collection<? extends SelectField<?>>)columns).from(getTableName()).where(condition)
          .orderBy(field(getKey())).limit(limit).offset(offset).getSQL(ParamType.INLINED);
    } else {
      sqlQuery = create.select().from(getTableName()).where(condition).orderBy(field(getKey())).limit(limit)
          .offset(offset).getSQL(ParamType.INLINED);
    }
    LOG.info("DSL Query: " + sqlQuery);
    return sqlQuery;
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
        if ((isPollerPartition && !isPolled) || !isPollerPartition) {
          pollRecords();
        }
      }
    }
  }

  @VisibleForTesting
  protected void setScheduledExecutorService(ScheduledExecutorService service)
  {
    scanService = service;
  }

  /**
   * Gets {@link WindowDataManager}
   *
   * @return windowDatatManager
   */
  public WindowDataManager getWindowManager()
  {
    return windowManager;
  }

  /**
   * Sets {@link WindowDataManager}
   *
   * @param windowDataManager
   */
  public void setWindowManager(WindowDataManager windowDataManager)
  {
    this.windowManager = windowDataManager;
  }

  /**
   * Gets non-polling static partitions count
   *
   * @return partitionCount
   */
  public int getPartitionCount()
  {
    return partitionCount;
  }

  /**
   * Sets non-polling static partitions count
   *
   * @param partitionCount
   */
  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

  /**
   * Returns the where clause
   *
   * @return whereCondition
   */
  public String getWhereCondition()
  {
    return whereCondition;
  }

  /**
   * Sets the where clause
   *
   * @param whereCondition
   */
  public void setWhereCondition(String whereCondition)
  {
    this.whereCondition = whereCondition;
  }

  /**
   * Returns the list of columns to select from the table
   *
   * @return columnsExpression
   */
  public String getColumnsExpression()
  {
    return columnsExpression;
  }

  /**
   * Comma separated list of columns to select from the given table
   *
   * @param columnsExpression
   */
  public void setColumnsExpression(String columnsExpression)
  {
    this.columnsExpression = columnsExpression;
  }

  /**
   * Returns the fetchsize for getting the results
   *
   * @return fetchSize
   */
  public int getFetchSize()
  {
    return fetchSize;
  }

  /**
   * Sets the fetchsize for getting the results
   *
   * @param fetchSize
   */
  public void setFetchSize(int fetchSize)
  {
    this.fetchSize = fetchSize;
  }

  /**
   * Returns the interval for polling the DB
   *
   * @return pollInterval
   */
  public int getPollInterval()
  {
    return pollInterval;
  }

  /**
   * Sets the interval for polling the DB
   *
   * @param pollInterval
   */
  public void setPollInterval(int pollInterval)
  {
    this.pollInterval = pollInterval;
  }

  /**
   * Returns the capacity of the emit queue
   *
   * @return queueCapacity
   */
  public int getQueueCapacity()
  {
    return queueCapacity;
  }

  /**
   * Sets the capacity of the emit queue
   *
   * @param queueCapacity
   */
  public void setQueueCapacity(int queueCapacity)
  {
    this.queueCapacity = queueCapacity;
  }

  /**
   * Returns the tableName which would be queried
   *
   * @return tableName
   */
  public String getTableName()
  {
    return tableName;
  }

  /**
   * Sets the tableName to query
   *
   * @param tableName
   */
  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  /**
   * Returns batchSize indicating the number of elements to emit in a bacth
   *
   * @return batchSize
   */
  public int getBatchSize()
  {
    return batchSize;
  }

  /**
   * Sets batchSize for number of elements to emit in a bacth
   *
   * @param batchSize
   */
  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  /**
   * Sets primary key column name
   *
   * @return key
   */
  public String getKey()
  {
    return key;
  }

  /**
   * gets primary key column name
   *
   * @param key
   */
  public void setKey(String key)
  {
    this.key = key;
  }

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcPollInputOperator.class);

}
