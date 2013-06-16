/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.hbase;

import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The base class for HBase output operators. <br>
 *
 * <br>
 * The output operator collects all the tuples that arrive in a window and writes them to
 * HBase in endWindow. The tuples are stored in an application specific manner. The concrete
 * implementation that extends this operator provides a method that specifies where to store the
 * tuple. The operator also stores the last processed window id into the table and loads it during setup time.
 * If the processing window id is not greater than the last processed window id that was loaded those tuples
 * are ignored till the processing window id becomes greater than the last processed window id.<br>
 *
 * <br>
 * @param <T> The tuple type
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public abstract class HBaseOutputOperator<T> extends HBaseOperatorBase implements Operator {

  private static final transient Logger logger = LoggerFactory.getLogger(HBaseOutputOperator.class);
  private static final String DEFAULT_LAST_WINDOW_PREFIX_COLUMN_NAME = "last_window";

  private transient String lastWindowColumnName;
  private transient byte[] lastWindowColumnBytes;

  private transient String appName;
  private transient String appId;
  private transient int operatorId;
  private transient List<T> tuples;
  // By default flush tuples only on end window
  private transient long lastProcessedWindow;
  private transient long currentWindow;

  private transient HBaseStatePersistenceStrategy persistenceStrategy;

  @InputPortFieldAnnotation(name="inputPort")
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>(this) {

    @Override
    public void process(T tuple)
    {
      if (currentWindow > lastProcessedWindow) {
        tuples.add(tuple);
      }
    }

  };

  public HBaseOutputOperator() {
    tuples = new ArrayList<T>();
    lastProcessedWindow = -1;
    currentWindow = 0;
    lastWindowColumnName = DEFAULT_LAST_WINDOW_PREFIX_COLUMN_NAME;
  }

  /**
   * Get the name of the column where the last processed window id is stored.
   * @return The column name
   */
  public String getLastWindowColumnName()
  {
    return lastWindowColumnName;
  }

   /**
   * Set the name of the column where the last processed window id is stored.
   * @return The column name
   */
  public void setLastWindowColumnName(String lastWindowColumnName)
  {
    this.lastWindowColumnName = lastWindowColumnName;
  }

  private void constructKeys() {
    String columnKey = appName + "_" + appId + "_" + operatorId + "_" + lastWindowColumnName;
    lastWindowColumnBytes = Bytes.toBytes(columnKey);
  }

  @Override
  public void setup(OperatorContext context)
  {
    try {
      appName = context.attrValue(DAG.APPLICATION_NAME, "HBaseOutputOperator");
      appId = context.attrValue(DAG.APPLICATION_ID, "AppId");
      operatorId = context.getId();
      constructKeys();
      setupConfiguration();
      persistenceStrategy = getPersistenceStrategy();
      persistenceStrategy.setTable(getTable());
      persistenceStrategy.setup();
      loadProcessState();
    }catch (IOException ie) {
      throw new RuntimeException(ie);
    }
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindow = windowId;
  }

  @Override
  public void endWindow()
  {
    try {
      processTuples();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Process the tuples that arrived in the window.
   * @throws IOException
   */
  private void processTuples() throws IOException {
    Iterator<T> it = tuples.iterator();
    while (it.hasNext()) {
      T t = it.next();
      try {
        processTuple(t);
      } catch (IOException e) {
        logger.error("Could not output tuple", e);
        throw new RuntimeException("Could not output tuple " + e.getMessage());
      }
      it.remove();
    }
    lastProcessedWindow = currentWindow;
    saveProcessState();
  }

  /**
   * Retrieve the processing state that was saved in a prior run.
   * The state is loaded from the HBase table in an application specific way.
   * @throws IOException
   */
  private void loadProcessState() throws IOException {
    byte[] lastProcessedWindowBytes = persistenceStrategy.getState(lastWindowColumnBytes);
    if (lastProcessedWindowBytes != null) {
      lastProcessedWindow = Bytes.toLong(lastProcessedWindowBytes);
    }
  }

  /**
   * Save the current processing state.
   * The state is saved to the HBase table in an application specific way.
   * @throws IOException
   */
  private void saveProcessState() throws IOException {
    byte[] lastProcessedWindowBytes = Bytes.toBytes(lastProcessedWindow);
    persistenceStrategy.saveState(lastWindowColumnBytes, lastProcessedWindowBytes);
  }

  /**
   * Get the persistence strategy.
   * Get the persistence strategy to use to save and retrieve state. The concrete class that
   * extends this calls should implement this method to specify how to save and load state.
   * @return The persistence strategy
   */
  public abstract HBaseStatePersistenceStrategy getPersistenceStrategy();

  /**
   * Process a tuple.
   * @param t The tuple
   * @throws IOException
   */
  public abstract void processTuple(T t) throws IOException;

}
