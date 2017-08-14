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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.AbstractStoreInputOperator;

import com.datatorrent.api.Context.OperatorContext;

/**
 * This is a base implementation of a JDBC input operator.&nbsp;
 * This operator reads data from a database through the JAVA DataBase Connectivity (JDBC) API
 * and emits the data as tuples.&nbsp;
 * Subclasses should implement the methods required to read the data from the database.
 * <p>
 * This is an abstract class. Sub-classes need to implement
 * {@link #queryToRetrieveData()} and {@link #getTuple(ResultSet)}.
 * </p>
 * @displayName Abstract JDBC Input
 * @category Input
 * @tags database, sql
 *
 * @param <T> The tuple type
 * @since 0.9.4
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractJdbcInputOperator<T> extends AbstractStoreInputOperator<T, JdbcStore>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractJdbcInputOperator.class);
  protected transient Statement queryStatement;

  /**
   * Any concrete class has to override this method to convert a Database row into Tuple.
   *
   * @param result a single row that has been read from database.
   * @return Tuple a tuples created from row which can be any Java object.
   */
  public abstract T getTuple(ResultSet result);

  /**
   * Any concrete class has to override this method to return the query string which will be used to
   * retrieve data from database.
   *
   * @return Query string
   */
  public abstract String queryToRetrieveData();

  /**
   * This executes the query to retrieve result from database.
   * It then converts each row into tuple and emit that into output port.
   */
  @Override
  public void emitTuples()
  {
    String query = queryToRetrieveData();
    logger.debug(String.format("select statement: %s", query));

    try {
      ResultSet result = queryStatement.executeQuery(query);
      if (result.next()) {
        do {
          T tuple = getTuple(result);
          outputPort.emit(tuple);
        }
        while (result.next());
      }
    } catch (SQLException ex) {
      store.disconnect();
      throw new RuntimeException(String.format("Error while running query: %s", query), ex);
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    try {
      queryStatement = store.getConnection().createStatement();
    } catch (SQLException e) {
      throw new RuntimeException("creating query", e);
    }
  }
}
