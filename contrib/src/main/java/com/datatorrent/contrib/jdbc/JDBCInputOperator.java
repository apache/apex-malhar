/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.lib.db.jdbc.AbstractJdbcInputOperator;

/**
 * JDBC input adapter operator, which reads data from persistence database through JAVA DataBase Connectivity (JDBC) API
 * and writes into output port(s). <p><br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: This has a single output port that receives data coming from database.<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * This is an abstract class. Class derived from this has to implement queryToRetrieveData() and getTuple() abstract methods.<br>
 * <br>
 * Run time checks:<br>
 * Following parameters have to be set while using this operator.<br>
 * dbUrl: URL to the database that this operator is going to write. This can not be null.<br>
 * dbDriver: JDBC driver for the database. This can not be null.<br>
 * tableName: If this adapter is writing only to a single table, table name has to be set here unless it is mentioned in column mapping.<br>
 * For writing to multiple table this field is ignored as the table names have to be specified in column mapping. See Column mapping field below for details.<br>
 * batchSize: This has to be at least 1 or more. If not specified the default batch size is 1000.<br>
 *
 * @since 0.3.2
 * @deprecated use {@link AbstractJdbcInputOperator}
 */
@Deprecated
public abstract class JDBCInputOperator<T> extends JDBCOperatorBase implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCInputOperator.class);
  Statement queryStatement = null;
  T lastEmittedTuple;
  long lastEmittedTimeStamp;

  public long getLastEmittedTimeStamp()
  {
    return lastEmittedTimeStamp;
  }

  public T getLastEmittedTuple()
  {
    return lastEmittedTuple;
  }

  public void setLastEmittedTuple(T lastEmittedTuple)
  {
    this.lastEmittedTuple = lastEmittedTuple;
  }

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
   * The output port that will emit tuple into DAG.
   */
  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  /**
   * This executes the query to retrieve result from database.
   * It then converts each row into tuple and emit that into output port.
   */
  public void emitTuples()
  {
    String query = queryToRetrieveData();
    logger.debug(String.format("select statement: %s", query));

    try {
      ResultSet result = queryStatement.executeQuery(query);
      while (result.next()) {
        T tuple = getTuple(result);
        outputPort.emit(tuple);
        // save a checkpoint how far is emitted
        lastEmittedTuple = tuple;
        lastEmittedTimeStamp = System.currentTimeMillis();
      }
    }
    catch (SQLException ex) {
      teardown();
      throw new RuntimeException(String.format("Error while running query: %s", query), ex);
    }
  }

  public void beginWindow(long windowId)
  {
  }

  public void endWindow()
  {
  }

  /**
   * This is the place to have initial setup for the operator. This creates JDBC connection to database.
   *
   * @param context
   */
  public void setup(OperatorContext context)
  {
    connect();
    try {
      queryStatement = connection.createStatement();
    }
    catch (SQLException ex) {
      disconnect();
      throw new RuntimeException("Error while creating select statement", ex);
    }
  }

  /**
   * Here close JDBC connection.
   */
  public void teardown()
  {
    disconnect();
  }

}
