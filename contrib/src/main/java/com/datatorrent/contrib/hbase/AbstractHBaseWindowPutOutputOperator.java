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

import javax.management.RuntimeErrorException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;

/**
 * Operator for storing tuples in HBase rows and provides a batch put. <br>
 * <p>
 * <br>
 * This class provides a HBase output operator that can be used to store tuples
 * in rows in a HBase table. It should be extended by the end-operator
 * developer. The extending class should implement operationPut method and
 * provide a HBase Put metric object that specifies where and what to store for
 * the tuple in the table.<br>
 *
 * <br>
 * This class provides a batch put where tuples are collected till the end
 * window and they are put on end window
 *
 * Note that since HBase doesn't support transactions this store cannot
 * guarantee each tuple is written only once to HBase in case the operator is
 * restarted from an earlier checkpoint. It only tries to minimize the number of
 * duplicates limiting it to the tuples that were processed in the window when
 * the operator shutdown.
 * It supports atleast once and atmost once processing modes.
 * Exactly once is not supported
 * @displayName: Abstract HBase Window Put Output Operator
 * @category: store
 * @tag: output operator, put, transactionable, batch
 * @param <T>
 *            The tuple type
 * @since 1.0.2
 */
public abstract class AbstractHBaseWindowPutOutputOperator<T> extends AbstractAggregateTransactionableStoreOutputOperator<T, HBaseWindowStore> {
  private static final transient Logger logger = LoggerFactory.getLogger(AbstractHBaseWindowPutOutputOperator.class);
  private List<T> tuples;
  private transient ProcessingMode mode;
  public ProcessingMode getMode()
  {
    return mode;
  }

  public void setMode(ProcessingMode mode)
  {
    this.mode = mode;
  }

  public AbstractHBaseWindowPutOutputOperator() {
    store = new HBaseWindowStore();
    tuples = new ArrayList<T>();
  }

  @Override
  public void storeAggregate() {
    HTable table = store.getTable();
    Iterator<T> it = tuples.iterator();
    while (it.hasNext()) {
      T t = it.next();
      try {
        Put put = operationPut(t);
        table.put(put);
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
    tuples.clear();
  }


  public abstract Put operationPut(T t) throws IOException;

  @Override
  public void processTuple(T tuple) {
    tuples.add(tuple);
  }

  @Override
  public void setup(OperatorContext context)
  {
    mode=context.getValue(context.PROCESSING_MODE);
    if(mode==ProcessingMode.EXACTLY_ONCE){
      throw new RuntimeException("This operator only supports atmost once and atleast once processing modes");
    }
    if(mode==ProcessingMode.AT_MOST_ONCE){
      tuples.clear();
    }
    super.setup(context);
  }

}
