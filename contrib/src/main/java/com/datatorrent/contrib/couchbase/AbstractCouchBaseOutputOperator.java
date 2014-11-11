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

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.AbstractAggregateTransactionableStoreOutputOperator;

import com.datatorrent.api.Context.OperatorContext;

import com.datatorrent.common.util.DTThrowable;

import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;

/**
 * AbstractCouchBaseOutputOperator which extends Transactionable Store Output Operator.
 * Classes extending from this operator should implement the abstract functionality of generateKey, getValue and insertOrUpdate.
 * Insert and Update couchbase operators extend from this class.
 * If the processing mode is EXACTLY ONCE, then in case of update the functionality will always be correct since it will overwrite the same keys
 * but for inserts there might be duplicates when operator fails.
 *
 */
public abstract class AbstractCouchBaseOutputOperator<T> extends AbstractAggregateTransactionableStoreOutputOperator<T, CouchBaseWindowStore>
{
  private static final transient Logger logger = LoggerFactory.getLogger(AbstractCouchBaseOutputOperator.class);
  private List<T> tuples;
  protected int numTuples;
  protected transient Latch countLatch ;
  protected CouchBaseSerializer serializer;

  public AbstractCouchBaseOutputOperator()
  {
    tuples = Lists.newArrayList();
    store = new CouchBaseWindowStore();
  }

  @Override
  public void setup(OperatorContext context)
  {
    ProcessingMode mode = context.getValue(context.PROCESSING_MODE);
    if (mode == ProcessingMode.AT_MOST_ONCE) {
      tuples.clear();
    }
    super.setup(context);
  }

  @Override
  public void beginWindow(long windowId){
     countLatch = new Latch(store.batchSize);
     numTuples = 0;
     super.beginWindow(windowId);
  }

  @Override
  public void processTuple(T tuple)
  {
    logger.info("in processTuple");
    logger.info("tuple is" + tuple.toString());
    tuples.add(tuple);
    setTuple(tuple);
    waitForBatch(false);
  }

  public void setTuple(T tuple)
  {
    numTuples++;
    logger.info("number of tuples is" + numTuples);
    String key = generateKey(tuple);
    logger.info("key is " + key);
    Object value = getValue(tuple);
    logger.info("value before serialization is " + value);
    if (serializer != null) {
      value = serializer.serialize(value);
    }
    logger.info("value after serialization is" + value);
    processKeyValue(key, value);
  }

  public List<T> getTuples()
  {
    return tuples;
  }

  @Override
  public void storeAggregate()
  {
    logger.info("in storeaggregate");
    waitForBatch(true);
  }

  public void waitForBatch(boolean endWindow)
  {
    if ((numTuples >= store.batchSize) && !endWindow) {
      try {
        logger.info("not in endwindow");
        countLatch.await(numTuples-store.batchSize);
      } catch (InterruptedException ex) {
        logger.error("Interrupted exception" + ex);
        DTThrowable.rethrow(ex);
      }
      tuples.clear();
    }
    else if(endWindow)
    {
       try {
        logger.info("In endwindow");
        countLatch.await(store.timeout);
      } catch (InterruptedException ex) {
        logger.error("Interrupted exception" + ex);
        DTThrowable.rethrow(ex);
      }
      tuples.clear();
    }

  }

  protected class CompletionListener implements OperationCompletionListener
  {
    @Override
    public void onComplete(OperationFuture<?> f) throws Exception
    {
      if (!((Boolean)f.get())) {
        throw new RuntimeException("Operation failed " + f);
      }
      countLatch.countDown();
    }
  }

  public static class CouchBaseJSONSerializer implements CouchBaseSerializer {

    private ObjectMapper mapper;

    public CouchBaseJSONSerializer() {
      mapper = new ObjectMapper();
    }

    @Override
    public String serialize(Object o)
    {
      String value = null;
      try {
        value = mapper.writeValueAsString(o);
      }
      catch (IOException ex) {
        logger.error("IO Exception", ex);
        DTThrowable.rethrow(ex);
      }
      return value;
    }
  }

  public CouchBaseSerializer getSerializer()
  {
    return serializer;
  }

  public void setSerializer(CouchBaseSerializer serializer)
  {
    this.serializer = serializer;
  }

  public abstract String generateKey(T tuple);

  public abstract Object getValue(T tuple);

  protected abstract void processKeyValue(String key, Object value);

  private class Latch {
  private final Object synchObj = new Object();
  private int count;

  public Latch(int noThreads) {
    synchronized (synchObj) {
      logger.info("count is " + count);
      this.count = noThreads;
    }
  }
  public void await(int difference) throws InterruptedException {
    synchronized (synchObj) {
     logger.info("store.batchsize is " + store.batchSize);
     logger.info("numTuples is " + numTuples);
      while (count > difference){
        logger.info("count is" + count);
        synchObj.wait();
      }
    }
  }
  public void countDown() {
    synchronized (synchObj) {
      logger.info("count is" + count);
      logger.info("store.batchsize is" + store.batchSize);
      logger.info("numTuples is " + numTuples);
      if (--count <= 0) {
      logger.info("count is" +count);
        synchObj.notifyAll();
      }
    }
  }
}
}

