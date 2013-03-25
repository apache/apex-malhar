/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.api.DAGContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
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

  public String getLastWindowColumnName()
  {
    return lastWindowColumnName;
  }

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
      appName = context.getApplicationAttributes().attrValue(DAG.STRAM_APPNAME, "HBaseOutputOperator");
      appId = context.getApplicationAttributes().attrValue(DAG.STRAM_APP_ID, "AppId");
      operatorId = context.getId();
      constructKeys();
      setupConfiguration();
      persistenceStrategy = getPersistenceStrategy();
      persistenceStrategy.setTable(getTable());
      persistenceStrategy.setup();
      loadProcessState();
    }catch (IOException ie) {
      new RuntimeException(ie);
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

  private void loadProcessState() throws IOException {
    byte[] lastProcessedWindowBytes = persistenceStrategy.getState(lastWindowColumnBytes);
    if (lastProcessedWindowBytes != null) {
      lastProcessedWindow = Bytes.toLong(lastProcessedWindowBytes);
    }
  }

  private void saveProcessState() throws IOException {
    byte[] lastProcessedWindowBytes = Bytes.toBytes(lastProcessedWindow);
    persistenceStrategy.saveState(lastWindowColumnBytes, lastProcessedWindowBytes);
  }

  public abstract HBaseStatePersistenceStrategy getPersistenceStrategy();

  public abstract void processTuple(T t) throws IOException;

}
