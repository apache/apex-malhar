/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;

/**
 * Operator for storing tuples in HBase columns.<br>
 *
 * <br>
 * This class provides a HBase output operator that can be used to store tuples
 * in columns in a HBase table. It should be extended by the end-operator
 * developer. The extending class should implement operationAppend method and
 * provide a HBase Append metric object that specifies where and what to store
 * for the tuple in the table.<br>
 *
 * <br>
 * This class provides batch append where tuples are collected till the end
 * window and they are appended on end window
 *
 * Note that since HBase doesn't support transactions this store cannot
 * guarantee each tuple is written only once to HBase in case the operator is
 * restarted from an earlier checkpoint. It only tries to minimize the number of
 * duplicates limiting it to the tuples that were processed in the window when
 * the operator shutdown.
 * BenchMark Results
 * -----------------
 * The operator operates at 20,000 tuples/sec with the following configuration
 * 
 * Container memory size=1G
 * CPU=Intel(R) Core(TM) i7-4500U CPU @ 1.80 GHz 2.40 Ghz
 * 
 * It supports atleast once and atmost once processing modes.
 * Exactly once is not supported
 * @param <T>
 *            The tuple type
 * @since 1.0.2
 */

public abstract class AbstractHBaseTransactionalAppendOutputOperator<T> extends
AbstractAggregateTransactionableStoreOutputOperator<T, HBaseTransactionalStore> {
  private static final transient Logger logger = LoggerFactory
      .getLogger(AbstractHBaseTransactionalAppendOutputOperator.class);
  private List<T> tuples;
  protected transient boolean atmostonceflag;
  private transient ProcessingMode mode;
  public ProcessingMode getMode()
  {
    return mode;
  }

  public void setMode(ProcessingMode mode)
  {
    this.mode = mode;
  }

  public AbstractHBaseTransactionalAppendOutputOperator() {
    store = new HBaseTransactionalStore();
    tuples = new ArrayList<T>();
  }

  @Override
  public void storeAggregate() {
    if(mode==ProcessingMode.AT_LEAST_ONCE){
      atleastOnce();
    }
    else if(mode==ProcessingMode.AT_MOST_ONCE){
      atmostOnce();
    }
    else{
      throw new RuntimeException("This operator only supports atleast once and atmost once cases");
    }
  }
  private void atmostOnce(){
    if(atmostonceflag){
      storeData();
    }
    tuples.clear();
  }
  private void atleastOnce()
  {
    storeData();
    tuples.clear();
  }
  private void storeData()
  {
    HTable table = store.getTable();
    Iterator<T> it = tuples.iterator();
    while (it.hasNext()) {
      T t = it.next();
      try {
        Append append = operationAppend(t);
        table.append(append);
      } catch (IOException e) {
        logger.error("Could not output tuple", e);
        DTThrowable.rethrow(e);
      }

    }
    try {
      table.flushCommits();
    } catch (RetriesExhaustedWithDetailsException e) {
      logger.error("Could not output tuple", e);
      DTThrowable.rethrow(e);
    } catch (InterruptedIOException e) {
      logger.error("Could not output tuple", e);
      DTThrowable.rethrow(e);
    }
  }

  /**
   * Return the HBase Append metric to store the tuple. The implementor should
   * return a HBase Append metric that specifies where and what to store for
   * the tuple in the table.
   * 
   * @param t
   *            The tuple
   * @return The HBase Append metric
   */
  public abstract Append operationAppend(T t);

  @Override
  public void processTuple(T tuple) {
    tuples.add(tuple);
  }
  @Override
  public void beginWindow(long windowId)
  {
    atmostonceflag=true;
    super.beginWindow(windowId);
  }
  @Override
  public void setup(OperatorContext context)
  {
    mode=context.getValue(context.PROCESSING_MODE);
    super.setup(context);
  }

}
