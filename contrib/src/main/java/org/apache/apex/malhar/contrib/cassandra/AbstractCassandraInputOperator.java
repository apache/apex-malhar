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
package org.apache.apex.malhar.contrib.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.AbstractStoreInputOperator;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Base input adapter which reads data from persistence database through DATASTAX API and writes into output port(s).&nbsp;
 * Subclasses should provide implementation to get tuples and querying to retrieve data.
 * <p>
 * This is an abstract class. Sub-classes need to implement {@link #queryToRetrieveData()} and {@link #getTuple(Row)}.
 * </p>
 * @param <T>
 * @displayName Abstract Cassandra Input
 * @category Input
 * @tags cassandra
 * @since 1.0.2
 */
public abstract class AbstractCassandraInputOperator<T> extends AbstractStoreInputOperator<T, CassandraStore>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractCassandraInputOperator.class);
  private PagingState nextPageState;
  private int fetchSize;
  int waitForDataTimeout = 100;

  @Override
  public void beginWindow(long l)
  {
    super.beginWindow(l);
  }

  /**
   * Get the amount of time to wait for data in milliseconds.
   * When there is no data available this timeout is used to throttle requests to the store so as to not continuously
   * slam it with requests. This is specified in milliseconds.
   *
   * @return The wait timeout in milliseconds
   */
  public int getWaitForDataTimeout()
  {
    return waitForDataTimeout;
  }

  /**
   * Set the amount of time to wait for data in milliseconds.
   * When there is no data available this timeout is used to throttle requests to the store so as to not continuously
   * slam it with requests. This is specified in milliseconds.
   *
   * @param waitForDataTimeout The wait timeout in milliseconds
   */
  public void setWaitForDataTimeout(int waitForDataTimeout)
  {
    this.waitForDataTimeout = waitForDataTimeout;
  }

  /**
   * Any concrete class has to override this method to convert a Database row into Tuple.
   *
   * @param row a single row that has been read from database.
   * @return Tuple a tuples created from row which can be any Java object.
   */
  public abstract T getTuple(Row row);

  /**
   * Any concrete class has to override this method to return the query string which will be used to
   * retrieve data from database.
   *
   * @return Query string
   */
  public abstract String queryToRetrieveData();

  /**
   * The output port that will emit tuple into DAG.
   */
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * This executes the query to retrieve result from database.
   * It then converts each row into tuple and emit that into output port.
   */
  @Override
  public void emitTuples()
  {
    String query = queryToRetrieveData();
    logger.debug("select statement: {}", query);

    SimpleStatement stmt = new SimpleStatement(query);
    stmt.setFetchSize(fetchSize);
    try {
      if (nextPageState != null) {
        stmt.setPagingState(nextPageState);
      }
      ResultSet result = store.getSession().execute(stmt);
      nextPageState = result.getExecutionInfo().getPagingState();

      if (!result.isExhausted()) {
        for (Row row : result) {
          T tuple = getTuple(row);
          emit(tuple);
        }
      } else {
        // No rows available wait for some time before retrying so as to not continuously slam the database
        Thread.sleep(waitForDataTimeout);
      }
    } catch (Exception ex) {
      store.disconnect();
      DTThrowable.rethrow(ex);
    }
  }

  protected void emit(T tuple)
  {
    outputPort.emit(tuple);
  }

  /**
   * Get page fetch Size
   * @return
   */
  public int getFetchSize()
  {
    return fetchSize;
  }

  /**
   * Set page fetch size
   * @param fetchSize
   */
  public void setFetchSize(int fetchSize)
  {
    this.fetchSize = fetchSize;
  }

}
