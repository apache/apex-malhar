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

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
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
import com.datatorrent.api.Operator.IdleTimeHandler;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.lib.db.AbstractStoreInputOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.KryoCloneUtils;
import com.datatorrent.netlet.util.DTThrowable;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.jooq.impl.DSL.field;

/**
 * Abstract operator for for consuming data using JDBC interface<br>
 * User needs to provide tableName, dbConnection, columnsExpression, look-up key <br>
 * Optionally batchSize, pollInterval and a where clause can be given <br>
 * This operator uses static partitioning to arrive at range queries for exactly
 * once reads<br>
 * This operator will create a configured number of non-polling static
 * partitions for fetching the existing data in the table. And an additional
 * single partition for polling additive data. Assumption is that there is an
 * ordered column using which range queries can be formed<br>
 * If an emitColumnList is provided, please ensure that the keyColumn is the
 * first column in the list<br>
 * Range queries are formed using the {@link JdbcMetaDataUtility} Output - comma
 * separated list of the emit columns eg columnA,columnB,columnC<br>
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
public abstract class AbstractJdbcPollInputOperator<T> extends AbstractStoreInputOperator<T, JdbcStore> implements
    ActivationListener<OperatorContext>, IdleTimeHandler, Partitioner<AbstractJdbcPollInputOperator<T>>
{
  private static int DEFAULT_QUEUE_CAPACITY = 4 * 1024 * 1024;
  private static int DEFAULT_POLL_INTERVAL = 10 * 1000;
  private static int DEFAULT_FETCH_SIZE = 20000;
  /**
   * poll interval in milliseconds
   */
  private int pollInterval = DEFAULT_POLL_INTERVAL;
  private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
  private int fetchSize = DEFAULT_FETCH_SIZE;

  @Min(1)
  private int partitionCount = 1;
  protected transient int operatorId;
  protected boolean isPollerPartition;
  protected int batchSize;

  /**
   * Map of windowId to <lower bound,upper bound> of the range key
   */
  protected transient MutablePair<Integer, Integer> currentWindowRecoveryState;
  private transient DSLContext create;

  private transient volatile boolean execute;
  private transient AtomicReference<Throwable> cause;
  private String tableName;
  private String key;
  private String columnsExpression;
  private String whereCondition = null;
  protected transient int spinMillis;
  protected long currentWindowId;
  protected KeyValPair<Integer, Integer> rangeQueryPair;
  protected Integer lower;
  protected transient boolean isPolled;
  protected transient Integer lastPolledBound;
  protected transient Integer lastEmittedRecord;
  protected transient ScheduledExecutorService scanService;
  protected transient LinkedBlockingDeque<T> emitQueue;
  protected transient PreparedStatement ps;
  protected WindowDataManager windowManager;

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
      scanService = Executors.newScheduledThreadPool(partitionCount);
    }
    spinMillis = context.getValue(Context.OperatorContext.SPIN_MILLIS);
    execute = true;
    cause = new AtomicReference<Throwable>();
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
        ps = store.getConnection().prepareStatement(
            buildRangeQuery(getKey(), rangeQueryPair.getKey(), Integer.MAX_VALUE), TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
      } else {
        ps = store.getConnection().prepareStatement(
            buildRangeQuery(getKey(), rangeQueryPair.getKey(), (rangeQueryPair.getValue() - rangeQueryPair.getKey())),
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
    lower = lastEmittedRecord;
  }

  private void updatePollQuery()
  {
    if ((lastPolledBound != lastEmittedRecord)) {
      if (lastEmittedRecord == null) {
        lastPolledBound = getRangeQueryPair().getKey();
      } else {
        lastPolledBound = lastEmittedRecord;
      }
      try {
        ps = store.getConnection().prepareStatement(buildRangeQuery(getKey(), lastPolledBound, Integer.MAX_VALUE),
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
      emitTuple(obj);
      lastEmittedRecord++;
    }
  }

  protected void emitTuple(T obj)
  {
    outputPort.emit(obj);
  }

  @Override
  public void endWindow()
  {
    try {
      if (currentWindowId > windowManager.getLargestRecoveryWindow()) {
        if (currentWindowRecoveryState == null) {
          currentWindowRecoveryState = new MutablePair<Integer, Integer>();
        }
        MutablePair<Integer, Integer> windowBoundaryPair = new MutablePair<>(lower, lastEmittedRecord);
        currentWindowRecoveryState = windowBoundaryPair;
        windowManager.save(currentWindowRecoveryState, operatorId, currentWindowId);
      }
    } catch (IOException e) {
      throw new RuntimeException("saving recovery", e);
    }
    currentWindowRecoveryState = new MutablePair<>();
  }

  @Override
  public void deactivate()
  {
    scanService.shutdownNow();
    store.disconnect();
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
          emitQueue.add(getTuple(result));
        } while (result.next());
      }
      isPolled = true;
    } catch (SQLException ex) {
      throw new RuntimeException(String.format("Error while running query"), ex);
    } finally {
      store.disconnect();
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
            buildRangeQuery(getKey(), getRangeQueryPair().getKey(),
                (getRangeQueryPair().getValue() - getRangeQueryPair().getKey())), TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
        LOG.info("Query formed to recover data - {}", ps.toString());

        emitReplayedTuples(ps);

      }

      if (currentWindowId == windowManager.getLargestRecoveryWindow()) {
        try {
          if (!isPollerPartition && rangeQueryPair.getValue() != null) {
            ps = store.getConnection().prepareStatement(
                buildRangeQuery(getKey(), lastEmittedRecord, (rangeQueryPair.getValue() - lastEmittedRecord)),
                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
          } else {
            Integer bound = null;
            if (lastEmittedRecord == null) {
              bound = getRangeQueryPair().getKey();
            } else {
              bound = lastEmittedRecord;
            }
            ps = store.getConnection().prepareStatement(buildRangeQuery(getKey(), bound, Integer.MAX_VALUE),
                TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
          }
          scanService.scheduleAtFixedRate(new DBPoller(), 0, pollInterval, TimeUnit.MILLISECONDS);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("replay", e);
    }

  }

  private boolean shouldReplayWindow(MutablePair<Integer, Integer> recoveredData)
  {
    if (recoveredData.left == null || recoveredData.right == null) {
      return false;
    }
    if (recoveredData.right.equals(rangeQueryPair.getValue()) || recoveredData.right.equals(lastEmittedRecord)) {
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
        lastEmittedRecord++;
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
        jdbcPoller.setRangeQueryPair(partitionToRangeMap.get(i));
        jdbcPoller.lastEmittedRecord = partitionToRangeMap.get(i).getKey();
      } else {
        // The upper bound for the n+1 partition is set to null since its a pollable partition
        int partitionKey = partitionToRangeMap.get(i - 1).getValue();
        jdbcPoller.setRangeQueryPair(new KeyValPair<Integer, Integer>(partitionKey, null));
        jdbcPoller.lastEmittedRecord = partitionKey;
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
  public String buildRangeQuery(String keyColumn, int offset, int limit)
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
          .limit(limit).offset(offset).getSQL(ParamType.INLINED);
    } else {
      sqlQuery = create.select().from(getTableName()).where(condition).limit(limit).offset(offset)
          .getSQL(ParamType.INLINED);
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
        try {
          if ((isPollerPartition && !isPolled) || !isPollerPartition) {
            pollRecords();
          }
        } catch (Exception ex) {
          cause.set(ex);
          execute = false;
        }
      }
    }
  }

  @VisibleForTesting
  protected void setScheduledExecutorService(ScheduledExecutorService service)
  {
    scanService = service;
  }

  public WindowDataManager getWindowManager()
  {
    return windowManager;
  }

  public void setWindowManager(WindowDataManager windowManager)
  {
    this.windowManager = windowManager;
  }

  public int getPartitionCount()
  {
    return partitionCount;
  }

  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }

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
  public String getColumnsExpression()
  {
    return columnsExpression;
  }

  /**
   * Comma separated list of columns to select from the given table
   */
  public void setColumnsExpression(String columnsExpression)
  {
    this.columnsExpression = columnsExpression;
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
  public KeyValPair<Integer, Integer> getRangeQueryPair()
  {
    return rangeQueryPair;
  }

  /**
   * Sets the rangeQueryPair <lowerBound,upperBound>
   */
  public void setRangeQueryPair(KeyValPair<Integer, Integer> rangeQueryPair)
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

  private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcPollInputOperator.class);

}
