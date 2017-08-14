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
package org.apache.apex.malhar.lib.db.jdbc;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.JDBCUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.AbstractStoreInputOperator;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.util.KryoCloneUtils;
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
import com.datatorrent.common.util.BaseOperator;

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
 * The operator uses jOOQ to build the SQL queries based on the discovered {@link org.jooq.SQLDialect}.
 * Note that some of the dialects (including Oracle) are only available in commercial
 * jOOQ distributions. If the dialect is not available, a generic translation is applied,
 * you can post-process the generated SQL by overriding {@link #buildRangeQuery(Object, int, int)}.
 *
 * @displayName Jdbc Polling Input Operator
 * @category Input
 * @tags database, sql, jdbc, partitionable, idempotent, pollable
 *
 * @since 3.5.0
 */
@Evolving
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public abstract class AbstractJdbcPollInputOperator<T> extends AbstractStoreInputOperator<T, JdbcStore> implements
    ActivationListener<OperatorContext>, Partitioner<AbstractJdbcPollInputOperator<T>>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcPollInputOperator.class);
  private static int DEFAULT_QUEUE_CAPACITY = 4 * 1024;
  private static int DEFAULT_POLL_INTERVAL = 10 * 1000;
  private static int DEFAULT_FETCH_SIZE = 20000;
  private static int DEFAULT_BATCH_SIZE = 2000;
  private static int DEFAULT_SLEEP_TIME = 100;
  private static int DEFAULT_RESULT_LIMIT = 100000;
  private int pollInterval = DEFAULT_POLL_INTERVAL; //in milliseconds
  private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
  private int fetchSize = DEFAULT_FETCH_SIZE;
  /**
   * Parameter to limit the number of results to fetch in one query by the Poller partition.
   */
  private int resultLimit = DEFAULT_RESULT_LIMIT;

  @Min(0)
  private int partitionCount = 0;
  private int batchSize = DEFAULT_BATCH_SIZE;

  @NotNull
  private String tableName;
  private String columnsExpression;
  @NotNull
  private String key;
  private String whereCondition = null;
  private long currentWindowId;
  private WindowDataManager windowManager;
  protected WindowData currentWindowRecoveryState;
  private boolean rebaseOffset;

  protected KeyValPair<Integer, Integer> rangeQueryPair;
  protected Integer lastEmittedRow;
  protected transient DSLContext dslContext;
  private transient volatile boolean execute;
  private transient ScheduledExecutorService scanService;
  private transient ScheduledFuture<?> pollFuture;
  protected transient LinkedBlockingQueue<T> emitQueue;
  protected transient PreparedStatement ps;
  protected boolean isPollerPartition;

  private transient int lastOffset;
  private transient Object prevKey;
  private transient Object lastKey;

  /**
   * The candidate key/offset pair identified by the poller thread
   * that, once emitted, can be used to rebase the lower bound for subsequent queries.
   */
  private transient AtomicReference<MutablePair<Object, Integer>> fetchedKeyAndOffset = new AtomicReference<>();

  /**
   * Signal to the fetch thread to rebase query.
   */
  private transient AtomicBoolean adjustKeyAndOffset = new AtomicBoolean();

  public AbstractJdbcPollInputOperator()
  {
    currentWindowRecoveryState = new WindowData();
    windowManager = new FSWindowDataManager();
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    dslContext = createDSLContext();
    if (scanService == null) {
      scanService = Executors.newScheduledThreadPool(1);
    }
    execute = true;
    emitQueue = new LinkedBlockingQueue<>(queueCapacity);
    windowManager.setup(context);
  }

  protected DSLContext createDSLContext()
  {
    return DSL.using(store.getConnection(), JDBCUtils.dialect(store.getDatabaseUrl()));
  }

  @Override
  public void activate(OperatorContext context)
  {
    long largestRecoveryWindow = windowManager.getLargestCompletedWindow();
    if (largestRecoveryWindow == Stateless.WINDOW_ID
        || context.getValue(Context.OperatorContext.ACTIVATION_WINDOW_ID) > largestRecoveryWindow) {
      initializePreparedStatement();
      schedulePollTask();
    }
  }

  private class DBPoller implements Runnable
  {
    @Override
    public void run()
    {
      pollRecords();
    }
  }

  private void schedulePollTask()
  {
    if (isPollerPartition) {
      pollFuture = scanService.scheduleAtFixedRate(new DBPoller(), 0, pollInterval, TimeUnit.MILLISECONDS);
    } else {
      LOG.debug("Scheduling for one time execution.");
      pollFuture = scanService.schedule(new DBPoller(), 0, TimeUnit.MILLISECONDS);
    }
  }

  protected void initializePreparedStatement()
  {
    try {
      if (currentWindowRecoveryState.lowerBound == 0 && currentWindowRecoveryState.key == null) {
        lastOffset = rangeQueryPair.getKey();
      } else {
        lastOffset = currentWindowRecoveryState.lowerBound;
        lastKey = currentWindowRecoveryState.key;
      }
      if (!isPollerPartition) {
        ps = store.getConnection().prepareStatement(
            buildRangeQuery(null, lastOffset, (rangeQueryPair.getValue() - lastOffset)),
            TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    if (currentWindowId <= windowManager.getLargestCompletedWindow()) {
      try {
        replay(currentWindowId);
        return;
      } catch (SQLException e) {
        throw new RuntimeException("Replay failed", e);
      }
    }

    currentWindowRecoveryState = WindowData.of(currentWindowRecoveryState.key, lastEmittedRow, 0);
    if (isPollerPartition) {
      MutablePair<Object, Integer> keyOffset = fetchedKeyAndOffset.get();
      if (keyOffset != null && keyOffset.getRight() < lastEmittedRow) {
        if (!adjustKeyAndOffset.get()) {
          // rebase offset
          lastEmittedRow -= keyOffset.getRight();
          currentWindowRecoveryState.lowerBound = lastEmittedRow;
          currentWindowRecoveryState.key = keyOffset.getLeft();
          adjustKeyAndOffset.set(true);
        }
      }
    }
  }

  @Override
  public void emitTuples()
  {
    if (currentWindowId <= windowManager.getLargestCompletedWindow()) {
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

  /**
   * Visible to subclasses to allow for custom offset saving and initialization.
   */
  protected static class WindowData
  {
    // members visible for access in subclasses of poll operator
    public Object key;
    public int lowerBound;
    public int upperBound;

    public static WindowData of(Object key, int lowerBound, int upperBound)
    {
      WindowData wd = new WindowData();
      wd.key = key;
      wd.lowerBound = lowerBound;
      wd.upperBound = upperBound;
      return wd;
    }
  }

  @Override
  public void endWindow()
  {
    if (pollFuture != null && (pollFuture.isCancelled() || pollFuture.isDone())) {
      try {
        pollFuture.get();
      } catch (Exception e) {
        throw new RuntimeException("JDBC thread failed", e);
      }

      if (isPollerPartition) {
        throw new IllegalStateException("poller task terminated");
      } else {
        // exit static query partition
        BaseOperator.shutdown();
      }
    }

    try {
      if (currentWindowId > windowManager.getLargestCompletedWindow()) {
        currentWindowRecoveryState.upperBound = lastEmittedRow;
        windowManager.save(currentWindowRecoveryState, currentWindowId);
      }
    } catch (IOException e) {
      throw new RuntimeException("saving recovery", e);
    }
  }

  @Override
  public void deactivate()
  {
    execute = false;
    scanService.shutdownNow();
    store.disconnect();
  }

  protected Object extractKey(ResultSet rs) throws SQLException
  {
    return rs.getObject(this.key);
  }

  /**
   * Execute the query and transfer results to the emit queue.
   * @param preparedStatement PreparedStatement to execute the query and fetch results.
   */
  protected int insertDbDataInQueue(PreparedStatement preparedStatement) throws SQLException, InterruptedException
  {
    int resultCount = 0;
    preparedStatement.setFetchSize(getFetchSize());
    ResultSet result = preparedStatement.executeQuery();
    while (execute && result.next()) {
      T obj = getTuple(result);
      if (obj == null) {
        continue;
      }
      while (execute && !emitQueue.offer(obj)) {
        Thread.sleep(DEFAULT_SLEEP_TIME);
      }
      if (isPollerPartition && rebaseOffset) {
        if (prevKey == null) {
          prevKey = extractKey(result);
        } else if (this.fetchedKeyAndOffset.get() == null) {
          // track key change
          Object nextKey = extractKey(result);
          if (!nextKey.equals(prevKey)) {
            // new key, ready for rebase (WHERE key > ?)
            fetchedKeyAndOffset.set(new MutablePair<>(prevKey, lastOffset + resultCount));
          }
        }
      }
      resultCount++;
    }
    result.close();
    preparedStatement.close();
    return resultCount;
  }

  /**
   * Fetch results from JDBC and transfer to queue.
   */
  protected void pollRecords()
  {
    try {
      if (isPollerPartition) {
        if (adjustKeyAndOffset.get()) {
          LOG.debug("lastOffset {} lastKey {} rebase {}", lastOffset, lastKey, fetchedKeyAndOffset.get());
          lastOffset -= fetchedKeyAndOffset.get().getRight();
          lastKey = fetchedKeyAndOffset.get().getLeft();
          prevKey = null;
          fetchedKeyAndOffset.set(null);
          adjustKeyAndOffset.set(false);
        }
        int count = getRecordsCount(lastKey);
        LOG.debug("Poll count {}", count);
        while (lastOffset < count) {
          PreparedStatement preparedStatement = store.getConnection().prepareStatement(buildRangeQuery(lastKey, lastOffset, resultLimit),
              TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
          lastOffset += insertDbDataInQueue(preparedStatement);
        }
      } else {
        insertDbDataInQueue(ps);
      }
    } catch (SQLException | InterruptedException ex) {
      throw new RuntimeException(ex);
    } finally {
      if (!isPollerPartition) {
        LOG.debug("fetched all records, marking complete.");
        execute = false;
      }
    }
  }

  public abstract T getTuple(ResultSet result);

  protected void replay(long windowId) throws SQLException
  {
    try {
      WindowData wd = (WindowData)windowManager.retrieve(windowId);
      if (wd != null && wd.upperBound - wd.lowerBound > 0) {
        LOG.debug("[Recovering Window ID - {} for key: {} record range: {}, {}]", windowId, wd.key,
            wd.lowerBound, wd.upperBound);
        ps = store.getConnection().prepareStatement(
            buildRangeQuery(wd.key, wd.lowerBound, (wd.upperBound - wd.lowerBound)), TYPE_FORWARD_ONLY,
            CONCUR_READ_ONLY);
        LOG.info("Query formed to recover data - {}", ps.toString());
        emitReplayedTuples(ps);
      }

      if (currentWindowId == windowManager.getLargestCompletedWindow()) {
        currentWindowRecoveryState = WindowData.of(wd.key, wd.upperBound, wd.upperBound);
        initializePreparedStatement();
        schedulePollTask();
      }
    } catch (IOException e) {
      throw new RuntimeException("Exception during replay of records.", e);
    }

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
    List<Partition<AbstractJdbcPollInputOperator<T>>> newPartitions = new ArrayList<>(
        getPartitionCount());

    final List<KeyValPair<Integer, Integer>> partitionRanges;
    try {
      store.connect();
      dslContext = createDSLContext();
      partitionRanges = getPartitionedQueryRanges(getPartitionCount());
    } catch (SQLException e) {
      LOG.error("Exception in initializing the partition range", e);
      throw new RuntimeException(e);
    } finally {
      store.disconnect();
    }

    KryoCloneUtils<AbstractJdbcPollInputOperator<T>> cloneUtils = KryoCloneUtils.createCloneUtils(this);
    int pollOffset = 0;

    // The n given partitions are for range queries and n + 1 partition is for polling query
    for (KeyValPair<Integer, Integer> range : partitionRanges) {
      AbstractJdbcPollInputOperator<T> jdbcPoller = cloneUtils.getClone();
      jdbcPoller.rangeQueryPair = range;
      jdbcPoller.lastEmittedRow = range.getKey();
      jdbcPoller.isPollerPartition = false;
      newPartitions.add(new DefaultPartition<>(jdbcPoller));
      pollOffset = range.getValue();
    }

    // The upper bound for the n+1 partition is set to null since its a pollable partition
    AbstractJdbcPollInputOperator<T> jdbcPoller = cloneUtils.getClone();
    jdbcPoller.rangeQueryPair = new KeyValPair<>(pollOffset, null);
    jdbcPoller.lastEmittedRow = pollOffset;
    jdbcPoller.isPollerPartition = true;
    newPartitions.add(new DefaultPartition<>(jdbcPoller));

    return newPartitions;
  }

  @Override
  public void partitioned(
      Map<Integer, com.datatorrent.api.Partitioner.Partition<AbstractJdbcPollInputOperator<T>>> partitions)
  {
    // Nothing to implement here
  }

  private List<KeyValPair<Integer, Integer>> getPartitionedQueryRanges(int partitions)
      throws SQLException
  {
    if (partitions == 0) {
      return new ArrayList<>(0);
    }

    int rowCount = 0;
    try {
      rowCount = getRecordsCount(null);
    } catch (SQLException e) {
      LOG.error("Exception in getting the record range", e);
    }

    List<KeyValPair<Integer, Integer>> partitionToQueryList = new ArrayList<>();
    int events = (rowCount / partitions);
    for (int i = 0, lowerOffset = 0, upperOffset = events; i < partitions - 1; i++, lowerOffset += events, upperOffset += events) {
      partitionToQueryList.add(new KeyValPair<>(lowerOffset, upperOffset));
    }

    partitionToQueryList.add(new KeyValPair<>(events * (partitions - 1), rowCount));
    LOG.info("Partition ranges - " + partitionToQueryList.toString());
    return partitionToQueryList;
  }

  protected Condition andLowerBoundKeyCondition(Condition c, Object lowerBoundKey)
  {
    return c.and(this.key + " > ?", lowerBoundKey);
  }

  /**
   * Finds the total number of rows in the table
   *
   * @return number of records in table
   */
  private int getRecordsCount(Object lowerBoundKey) throws SQLException
  {
    Condition condition = DSL.trueCondition();
    if (getWhereCondition() != null) {
      condition = condition.and(getWhereCondition());
    }

    if (isPollerPartition && lowerBoundKey != null) {
      condition = andLowerBoundKeyCondition(condition, lowerBoundKey);
    }

    int recordsCount = dslContext.select(DSL.count()).from(getTableName()).where(condition).fetchOne(0, int.class);
    return recordsCount;
  }

  /**
   * Helper function returns a range query based on the bounds passed<br>
   */
  protected String buildRangeQuery(Object lowerBoundKey, int offset, int limit)
  {
    Condition condition = DSL.trueCondition();
    if (getWhereCondition() != null) {
      condition = condition.and(getWhereCondition());
    }

    if (isPollerPartition && lowerBoundKey != null) {
      condition = andLowerBoundKeyCondition(condition, lowerBoundKey);
    }

    String sqlQuery;
    if (getColumnsExpression() != null) {
      Collection<Field<?>> columns = new ArrayList<>();
      for (String column : getColumnsExpression().split(",")) {
        columns.add(field(column));
      }
      sqlQuery = dslContext.select(columns).from(getTableName()).where(condition)
          .orderBy(field(getKey())).limit(limit).offset(offset).getSQL(ParamType.INLINED);
    } else {
      sqlQuery = dslContext.select().from(getTableName()).where(condition).orderBy(field(getKey())).limit(limit)
          .offset(offset).getSQL(ParamType.INLINED);
    }
    LOG.info("DSL Query: " + sqlQuery);
    return sqlQuery;
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
   * Sets non-polling static partitions count.<p>
   * When set to 0, the operator will run in poll mode only.
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

  /**
   * gets the Result Limit size, parameter to limit the number of results
   * to fetch in one query by the Poller partition.
   */
  public int getResultLimit()
  {
    return resultLimit;
  }

  /**
   * Sets the
   * @param resultLimit Parameter to limit the number of results to fetch in one query by the Poller partition.
   */
  public void setResultLimit(int resultLimit)
  {
    this.resultLimit = resultLimit;
  }

  public boolean isRebaseOffset()
  {
    return rebaseOffset;
  }

  /**
   * Whether the query should automatically be augmented with a WHERE
   * condition for trailing lower bound key value.
   * <p>
   * Rebase allows the operator to poll from tables where old data is
   * periodically purged. Without it, the default zero based row offset would
   * lead to missed data. The trailing floor is also more efficient when working
   * with key partitioned sources as the query planner can skip those partitions
   * below the base key.
   */
  public void setRebaseOffset(boolean rebaseOffset)
  {
    this.rebaseOffset = rebaseOffset;
  }

}
