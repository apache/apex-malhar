/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.jdbc;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
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
   * @param result
   * @return Tuple
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
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>(this);

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
      closeJDBCConnection();
      throw new RuntimeException(String.format("Error while running query: %s", query), ex);
    }
  }

  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
  }

  public void endWindow()
  {
  }

  public void setup(OperatorContext context)
  {
    setupJDBCConnection();
    try {
      queryStatement = connection.createStatement();
    }
    catch (SQLException ex) {
      closeJDBCConnection();
      throw new RuntimeException("Error while creating select statement", ex);
    }
  }

  public void teardown()
  {
    closeJDBCConnection();
  }

  /**
   * Not Needed for input operator. Only needed for output operator.
   * @param mapping
   */
  @Override
  protected void parseMapping(ArrayList<String> mapping)
  {
    // No op.
  }
}
