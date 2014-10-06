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
package com.datatorrent.contrib.couchbase;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.startScan_args._Fields;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractCouchBaseOutputOperator which extends Transactionable Store Output Operator.
 * Classes extending from this operator should implement the abstract functionality of generateKey, getObject and insertOrUpdate.
 * Insert and Update couchbase operators extend from this class.
 * If the processing mode is EXACTLY ONCE, then in case of update the functionality will always be correct since it will overwrite the same keys
 * but for inserts there might be duplicates when operator fails.
 *
 */
public abstract class AbstractCouchBaseOutputOperator<T> extends AbstractAggregateTransactionableStoreOutputOperator<T, CouchBaseWindowStore>
{
  private static final transient Logger logger = LoggerFactory.getLogger(AbstractCouchBaseOutputOperator.class);
  private List<T> tuples;
  private transient Operator.ProcessingMode mode;
  protected int num_tuples;
  protected CountDownLatch countLatch;
  protected boolean isEndWindow;

  public Operator.ProcessingMode getMode()
  {
    return mode;
  }

  public void setMode(Operator.ProcessingMode mode)
  {
    this.mode = mode;
  }

  public AbstractCouchBaseOutputOperator()
  {
    tuples = Lists.newArrayList();
    store = new CouchBaseWindowStore();
    countLatch = new CountDownLatch(store.batch_size);
  }

  @Override
  public void setup(OperatorContext context)
  {
    mode = context.getValue(context.PROCESSING_MODE);
    if (mode == ProcessingMode.AT_MOST_ONCE) {
      tuples.clear();
    }
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId){
     countLatch = new CountDownLatch(store.batch_size);
     num_tuples = 0;
     isEndWindow = false;
     super.beginWindow(windowId);
  }

  @Override
  public void processTuple(T tuple)
  {
    tuples.add(tuple);
    setTuple(tuple);
    if ((num_tuples < store.batch_size) || isEndWindow) {
      waitForBatch();
    }
  }

  public void setTuple(T tuple)
  {
    num_tuples++;
    String key = generateKey(tuple);
    Object input = getObject(tuple);
    ObjectMapper mapper = new ObjectMapper();
    String value = new String();
    try {
      value = mapper.writeValueAsString(input);
    }
    catch (IOException ex) {
      logger.error("IO Exception", ex);
      DTThrowable.rethrow(ex);
    }
    processTupleCouchbase(key, value);
  }

  public List<T> getTuples()
  {
    return tuples;
  }

  @Override
  public void storeAggregate()
  {
    isEndWindow = true;
    waitForBatch();
    tuples.clear();
  }

  public void waitForBatch()
  {
    try {
      countLatch.await();
    }
    catch (InterruptedException ex) {
      logger.error("Interrupted exception" + ex);
      DTThrowable.rethrow(ex);
    }
    tuples.clear();
  }

  public abstract String generateKey(T tuple);

  public abstract Object getObject(T tuple);

  protected abstract void processTupleCouchbase(String key, Object value);

}
