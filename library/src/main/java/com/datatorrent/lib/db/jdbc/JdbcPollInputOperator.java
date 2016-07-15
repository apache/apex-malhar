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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OperatorAnnotation;

/**
 * A concrete implementation for {@link AbstractJdbcPollInputOperator}} for
 * consuming data from MySQL using JDBC interface <br>
 * User needs to provide tableName,dbConnection,setEmitColumnList,look-up key
 * <br>
 * Optionally batchSize,pollInterval,Look-up key and a where clause can be given
 * <br>
 * This operator uses static partitioning to arrive at range queries for exactly
 * once reads<br>
 * Assumption is that there is an ordered column using which range queries can
 * be formed<br>
 * If an emitColumnList is provided, please ensure that the keyColumn is the
 * first column in the list<br>
 * Range queries are formed using the {@link JdbcMetaDataUtility}} Output -
 * comma separated list of the emit columns eg columnA,columnB,columnC
 * 
 * @displayName Jdbc Polling Input Operator
 * @category Input
 * @tags database, sql, jdbc
 */
@Evolving
@OperatorAnnotation(checkpointableWithinAppWindow = false)
public class JdbcPollInputOperator extends AbstractJdbcPollInputOperator<Object>
{
  private long lastBatchWindowId;
  private transient long currentWindowId;
  private long lastCreationTsMillis;
  private long fetchBackMillis = 0L;
  private ArrayList<String> emitColumns;
  private transient int count = 0;

  /**
   * Returns the emit columns
   */
  public List<String> getEmitColumns()
  {
    return emitColumns;
  }

  /**
   * Sets the emit columns
   */
  public void setEmitColumns(ArrayList<String> emitColumns)
  {
    this.emitColumns = emitColumns;
  }

  /**
   * Returns fetchBackMilis
   */
  public long getFetchBackMillis()
  {
    return fetchBackMillis;
  }

  /**
   * Sets fetchBackMilis - used in polling
   */
  public void setFetchBackMillis(long fetchBackMillis)
  {
    this.fetchBackMillis = fetchBackMillis;
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    parseEmitColumnList(getEmitColumnList());
    lastCreationTsMillis = System.currentTimeMillis() - fetchBackMillis;
  }

  private void parseEmitColumnList(String columnList)
  {
    String[] cols = columnList.split(",");
    ArrayList<String> arr = Lists.newArrayList();
    for (int i = 0; i < cols.length; i++) {
      arr.add(cols[i]);
    }
    setEmitColumns(arr);
  }

  @Override
  public void beginWindow(long l)
  {
    super.beginWindow(l);
    currentWindowId = l;
  }

  @Override
  protected void pollRecords(PreparedStatement ps)
  {
    ResultSet rs = null;
    List<Object> metaList = new ArrayList<>();

    if (isReplayed) {
      return;
    }

    try {
      if (ps.isClosed()) {
        LOG.debug("Returning due to closed ps for non-pollable partitions");
        return;
      }
    } catch (SQLException e) {
      LOG.error("Prepared statement is closed", e);
      throw new RuntimeException(e);
    }

    try (PreparedStatement pStat = ps;) {
      pStat.setFetchSize(getFetchSize());
      LOG.debug("sql query = {}", pStat);
      rs = pStat.executeQuery();
      boolean hasNext = false;

      if (rs == null || rs.isClosed()) {
        return;
      }

      while ((hasNext = rs.next())) {
        Object key = null;
        StringBuilder resultTuple = new StringBuilder();
        try {
          if (count < getBatchSize()) {
            key = rs.getObject(getKey());
            for (String obj : emitColumns) {
              resultTuple.append(rs.getObject(obj) + ",");
            }
            metaList.add(resultTuple.substring(0, resultTuple.length() - 1));
            count++;
          } else {
            emitQueue.add(metaList);
            metaList = new ArrayList<>();
            key = rs.getObject(getKey());
            for (String obj : emitColumns) {
              resultTuple.append(rs.getObject(obj) + ",");
            }
            metaList.add(resultTuple.substring(0, resultTuple.length() - 1));
            count = 0;
          }
        } catch (NullPointerException npe) {
          LOG.error("Key not found" + npe);
          throw new RuntimeException(npe);
        }
        if (isPollable) {
          highestPolled = key.toString();
          isPolled = true;
        }
      }
      /*Flush the remaining records once the result set is over and batch-size is not reached,
       * Dont flush if its pollable*/
      if (!hasNext) {
        if ((isPollable && isPolled) || !isPollable) {
          emitQueue.offer(metaList);
          metaList = new ArrayList<>();
          count = 0;
        }
        if (!isPolled) {
          isPolled = true;
        }
      }
      LOG.debug("last creation time stamp = {}", lastCreationTsMillis);
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void emitTuples()
  {
    if (isReplayed) {
      LOG.debug(
          "Calling emit tuples during window - " + currentWindowId + "::" + windowManager.getLargestRecoveryWindow());
      LOG.debug("Returning for replayed window");
      return;
    }

    List<Object> tuples;

    if (lastBatchWindowId < currentWindowId) {
      if ((tuples = emitQueue.poll()) != null) {
        for (Object tuple : tuples) {
          String[] str = tuple.toString().split(",");
          if (lower == null) {
            lower = str[0];
          }
          upper = str[0];
          outputPort.emit(tuple);
        }
        lastBatchWindowId = currentWindowId;
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(JdbcPollInputOperator.class);
}
